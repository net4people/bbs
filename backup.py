#!/usr/bin/env python3

# Usage: ./backup.py -u username:token net4people/bbs bbs-20201231.zip
#
# Downloads GitHub issues, comments, and labels using the GitHub REST API
# (https://docs.github.com/en/free-pro-team@latest/rest). Saves output to a zip
# file.
#
# The -u option controls authentication. You don't have to use it, but if you
# don't, you will be limited to 60 API requests per hour. When you are
# authenticated, you get 5000 API requests per hour. The "token" part is a
# Personal Access Token, created at https://github.com/settings/tokens.
# https://docs.github.com/en/free-pro-team@latest/github/authenticating-to-github/creating-a-personal-access-token
# You don't have to enable any scopes for the token.

import datetime
import getopt
import json
import os
import os.path
import sys
import tempfile
import time
import urllib.parse
import zipfile

import mistune
import requests

BASE_URL = "https://api.github.com/"

# https://docs.github.com/en/free-pro-team@latest/rest/overview/media-types
MEDIATYPE = "application/vnd.github.v3+json"
# https://docs.github.com/en/free-pro-team@latest/rest/reference/issues#list-repository-issues-preview-notices
MEDIATYPE_REACTIONS = "application/vnd.github.squirrel-girl-preview+json"

UNSET_ZIPINFO_DATE_TIME = zipfile.ZipInfo("").date_time

def url_origin(url):
    components = urllib.parse.urlparse(url)
    return (components.scheme, components.netloc)

def check_url_origin(base, url):
    assert url_origin(base) == url_origin(url), (base, url)

def datetime_to_zip_time(d):
    return (d.year, d.month, d.day, d.hour, d.minute, d.second)

def timestamp_to_zip_time(timestamp):
    return datetime_to_zip_time(datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%SZ"))

def http_date_to_zip_time(timestamp):
    # https://tools.ietf.org/html/rfc7231#section-7.1.1.1
    # We only support the IMF-fixdate format.
    return datetime_to_zip_time(datetime.datetime.strptime(timestamp, "%a, %d %b %Y %H:%M:%S GMT"))

# https://docs.github.com/en/free-pro-team@latest/rest/overview/resources-in-the-rest-api#rate-limiting
# Returns a datetime at which the rate limit will be reset, or None if not
# currently rate limited.
def rate_limit_reset(r):
    # A rate-limited response is one that has status code 403, an
    # x-ratelimit-remaining header with a value of 0, and an x-ratelimit-reset
    # header.
    if r.status_code != 403:
        return None

    remaining = r.headers.get("x-ratelimit-remaining")
    if remaining is None:
        return None
    try:
        if int(remaining) > 0:
            return None
    except ValueError:
        return None

    # If x-ratelimit-remaining is set, assume x-ratelimit-reset is set.
    reset = r.headers["x-ratelimit-reset"]
    return datetime.datetime.utcfromtimestamp(int(r.headers["x-ratelimit-reset"]))

def get(url, mediatype, auth, params={}):
    # TODO: warn on 301 redirect? https://docs.github.com/en/free-pro-team@latest/rest/overview/resources-in-the-rest-api#http-redirects

    kwargs = {}
    if auth is not None:
        kwargs["auth"] = auth

    while True:
        print(url, end="", flush=True)
        try:
            headers = {}
            if mediatype is not None:
                headers["Accept"] = mediatype
            r = requests.get(url, params=params, headers=headers, **kwargs)
        except Exception as e:
            print(f" => {str(type(e))}", flush=True)
            raise

        print(f" => {r.status_code} {r.reason} {r.headers.get('x-ratelimit-used', '-')}/{r.headers.get('x-ratelimit-limit', '-')}", flush=True)
        reset = rate_limit_reset(r)
        if reset is not None:
            reset_seconds = (reset - datetime.datetime.utcnow()).total_seconds()
            print(f"waiting {reset_seconds:.0f} s for rate limit, will resume at {reset.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
            time.sleep(reset_seconds)
        else:
            r.raise_for_status()
            return r

# https://docs.github.com/en/free-pro-team@latest/rest/overview/resources-in-the-rest-api#pagination
# https://docs.github.com/en/free-pro-team@latest/guides/traversing-with-pagination
def get_paginated(url, mediatype, auth, params={}):
    params = params.copy()
    try:
        del params["page"]
    except KeyError:
        pass
    params["per_page"] = "100"

    while True:
        r = get(url, mediatype, auth, params)
        yield r

        next_link = r.links.get("next")
        if next_link is None:
            break
        next_url = next_link["url"]
        # The API documentation instructs us to follow the "next" link without
        # interpretation, but at least ensure it refers to the same scheme and
        # host.
        check_url_origin(url, next_url)

        url = next_url

# If zi.date_time is None, then it will be replaced with the value of the HTTP
# response's Last-Modified header, if present.
def get_to_zipinfo(url, z, zi, mediatype, auth, params={}):
    r = get(url, mediatype, auth, params)

    if zi.date_time == UNSET_ZIPINFO_DATE_TIME:
        last_modified = r.headers.get("Last-Modified")
        if last_modified is not None:
            zi.date_time = http_date_to_zip_time(last_modified)

    with z.open(zi, mode="w") as f:
        for chunk in r.iter_content(4096):
            f.write(chunk)

# Converts a list of path components into a string path, raising an exception if
# any component contains a slash, is "." or "..", or is empty; or if the whole
# path is empty. The checks are to prevent any file writes outside the
# destination directory when the zip file is extracted. We rely on the
# assumption that no other files in the zip file are symbolic links, which is
# true because this program does not create symbolic links.
def make_zip_file_path(*components):
    for component in components:
        if "/" in component:
            raise ValueError("path component contains a slash")
        if component == "":
            raise ValueError("path component is empty")
        if component == ".":
            raise ValueError("path component is a self directory reference")
        if component == "..":
            raise ValueError("path component is a parent directory reference")
    if not components:
        raise ValueError("path is empty")
    return "/".join(components)

# Custom mistune.Renderer that stores a list of all links encountered.
class LinkExtractionRenderer(mistune.Renderer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.links = []

    def autolink(self, link, is_email=False):
        self.links.append(link)
        return super().autolink(link, is_email)

    def image(self, src, title, alt_text):
        self.links.append(src)
        return super().image(src, title, alt_text)

    def link(self, link, title, content):
        self.links.append(link)
        return super().link(link, title, content)

def markdown_extract_links(markdown):
    renderer = LinkExtractionRenderer()
    mistune.Markdown(renderer=renderer)(markdown) # Discard HTML output.
    return renderer.links

# Return seq with prefix stripped if it has such a prefix, or else None.
def strip_prefix(seq, prefix):
    if len(seq) < len(prefix):
        return None
    for a, b in zip(seq, prefix):
        if a != b:
            return None
    return seq[len(prefix):]

def split_url_path(path):
    return tuple(urllib.parse.unquote(component) for component in path.split("/"))

def strip_url_path_prefix(path, prefix):
    return strip_prefix(split_url_path(path), split_url_path(prefix))

# If url is one we want to download, return a list of path components for the
# path we want to store it at.
def link_is_wanted(url):
    try:
        components = urllib.parse.urlparse(url)
    except ValueError:
        return None

    if components.scheme == "https" and components.netloc == "user-images.githubusercontent.com":
        subpath = strip_url_path_prefix(components.path, "")
        if subpath is not None:
            # Inline image.
            return ("user-images.githubusercontent.com", *subpath)
    if components.scheme == "https" and components.netloc == "github.com":
        subpath = strip_url_path_prefix(components.path, f"/{owner}/{repo}/files")
        if subpath is not None:
            # File attachment.
            return ("files", *subpath)

def backup(owner, repo, z, auth):
    paths_seen = set()
    # Calls make_zip_file_path, and additional raises an exception if the path
    # has already been used.
    def check_path(*components):
        path = make_zip_file_path(*components)
        if path in paths_seen:
            raise ValueError(f"duplicate filename {path!a}")
        paths_seen.add(path)
        return path

    # Escape owner and repo suitably for use in a URL.
    owner = urllib.parse.quote(owner, safe="")
    repo = urllib.parse.quote(repo, safe="")

    now = datetime.datetime.utcnow()
    z.writestr(check_path("README"), f"""\
Archive of the GitHub repository https://github.com/{owner}/{repo}/
made {now.strftime("%Y-%m-%d %H:%M:%S")}.
""")

    file_urls = set()

    # https://docs.github.com/en/free-pro-team@latest/rest/reference/issues#list-repository-issues
    issues_url = urllib.parse.urlparse(BASE_URL)._replace(
        path=f"/repos/{owner}/{repo}/issues",
    ).geturl()
    for r in get_paginated(issues_url, MEDIATYPE_REACTIONS, auth, {"sort": "created", "direction": "asc"}):
        for issue in r.json():
            check_url_origin(BASE_URL, issue["url"])
            zi = zipfile.ZipInfo(check_path("issues", str(issue["id"]) + ".json"), timestamp_to_zip_time(issue["created_at"]))
            get_to_zipinfo(issue["url"], z, zi, MEDIATYPE_REACTIONS, auth)

            # Re-open the JSON file we just wrote, to parse it for links.
            with z.open(zi) as f:
                for link in markdown_extract_links(json.load(f)["body"]):
                    dest = link_is_wanted(link)
                    if dest is not None:
                        file_urls.add((dest, link))

            # There's no API for getting all reactions in a repository, so get
            # them per issue and per comment.
            # https://docs.github.com/en/free-pro-team@latest/rest/reference/reactions#list-reactions-for-an-issue
            reactions_url = issue["reactions"]["url"]
            check_url_origin(BASE_URL, reactions_url)
            for r2 in get_paginated(reactions_url, MEDIATYPE_REACTIONS, auth):
                for reaction in r2.json():
                    zi = zipfile.ZipInfo(check_path("issues", str(issue["id"]), "reactions", str(reaction["id"]) + ".json"), timestamp_to_zip_time(reaction["created_at"]))
                    with z.open(zi, mode="w") as f:
                        f.write(json.dumps(reaction).encode("utf-8"))

    # https://docs.github.com/en/free-pro-team@latest/rest/reference/issues#list-issue-comments-for-a-repository
    # Comments are linked to their parent issue via the issue_url field.
    comments_url = urllib.parse.urlparse(BASE_URL)._replace(
        path=f"/repos/{owner}/{repo}/issues/comments",
    ).geturl()
    for r in get_paginated(comments_url, MEDIATYPE_REACTIONS, auth):
        for comment in r.json():
            check_url_origin(BASE_URL, comment["url"])
            zi = zipfile.ZipInfo(check_path("issues", "comments", str(comment["id"]) + ".json"), timestamp_to_zip_time(comment["created_at"]))
            get_to_zipinfo(comment["url"], z, zi, MEDIATYPE_REACTIONS, auth)

            # Re-open the JSON file we just wrote, to parse it for links.
            with z.open(zi) as f:
                for link in markdown_extract_links(json.load(f)["body"]):
                    dest = link_is_wanted(link)
                    if dest is not None:
                        file_urls.add((dest, link))

            # There's no API for getting all reactions in a repository, so get
            # them per issue and per comment.
            # https://docs.github.com/en/free-pro-team@latest/rest/reference/reactions#list-reactions-for-an-issue-comment
            reactions_url = comment["reactions"]["url"]
            check_url_origin(BASE_URL, reactions_url)
            for r2 in get_paginated(reactions_url, MEDIATYPE_REACTIONS, auth):
                for reaction in r2.json():
                    zi = zipfile.ZipInfo(check_path("issues", "comments", str(comment["id"]), "reactions", str(reaction["id"]) + ".json"), timestamp_to_zip_time(reaction["created_at"]))
                    with z.open(zi, mode="w") as f:
                        f.write(json.dumps(reaction).encode("utf-8"))

    labels_url = urllib.parse.urlparse(BASE_URL)._replace(
        path=f"/repos/{owner}/{repo}/labels",
    ).geturl()
    for r in get_paginated(labels_url, MEDIATYPE, auth):
        for label in r.json():
            check_url_origin(BASE_URL, label["url"])
            zi = zipfile.ZipInfo(check_path("labels", str(label["id"]) + ".json"))
            get_to_zipinfo(label["url"], z, zi, MEDIATYPE, auth)

    # TODO: avatars

    for dest, url in sorted(file_urls):
        zi = zipfile.ZipInfo(check_path(*dest))
        get_to_zipinfo(url, z, zi, None, None)

if __name__ == "__main__":
    auth = None

    opts, (repo, zip_filename) = getopt.gnu_getopt(sys.argv[1:], "u:")
    for o, a in opts:
        if o == "-u":
            username, token = a.split(":", 1)
            auth = requests.auth.HTTPBasicAuth(username, token)
        elif o in ("-h", "--help"):
            pass

    owner, repo = repo.split("/", 1)

    # Write to a temporary file, then rename to the requested name when
    # finished.
    with tempfile.NamedTemporaryFile(dir=os.path.dirname(zip_filename), suffix=".zip", delete=False) as f:
        try:
            with zipfile.ZipFile(f, mode="w") as z:
                backup(owner, repo, z, auth)
            os.rename(f.name, zip_filename)
        except:
            # Delete output zip file on error.
            os.remove(f.name)
            raise
