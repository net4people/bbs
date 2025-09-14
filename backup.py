#!/usr/bin/env python3

# Usage: ./backup.py -u username:token net4people/bbs bbs-20201231.sqlite3
#
# Downloads GitHub issues, comments, and labels using the GitHub REST API
# (https://docs.github.com/en/rest?apiVersion=2022-11-28). Saves output to an
# SQLite database file.
#
# The SQLite database is used as a container for archiving API responses. The
# tables contain little more than an id column and a blob of JSON. They don't
# have broken-down columns for convenient querying. That said, you can get
# something like that using the SQLite JSON functions:
# https://sqlite.org/json1.html
# For example:
# sqlite3 bbs.sqlite3 "SELECT id, json_extract(json,'$.html_url') AS html_url FROM issues"
#
# The -u option controls authentication. You don't have to use it, but if you
# don't, you will be limited to 60 API requests per hour. When you are
# authenticated, you get 5000 API requests per hour. The "token" part is a
# Personal Access Token, created at https://github.com/settings/tokens.
# https://docs.github.com/en/rest/authentication/authenticating-to-the-rest-api?apiVersion=2022-11-28#authenticating-with-a-personal-access-token
# You don't have to enable any scopes for the token.
#
# It should be possible to interrupt the backup process, resulting in a partial
# database file, and resume it using the same database file. The backup is
# complete when this program exits with a 0 return code.

import datetime
import getopt
import json
import os.path
import shutil
import sqlite3
import sys
import tempfile
import time
import urllib.parse

import mistune
import requests

BASE_URL = "https://api.github.com/"

# https://docs.github.com/en/rest/using-the-rest-api/getting-started-with-the-rest-api?apiVersion=2022-11-28#media-types
MEDIATYPE = "application/vnd.github+json"

def url_origin(url):
    components = urllib.parse.urlparse(url)
    return (components.scheme, components.netloc)

def check_url_origin(base, url):
    assert url_origin(base) == url_origin(url), (base, url)

# https://docs.github.com/en/rest/using-the-rest-api/rate-limits-for-the-rest-api?apiVersion=2022-11-28#checking-the-status-of-your-rate-limit
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

def response_datetime(r):
    dt = r.headers.get("date")
    return datetime.datetime.strptime(dt, "%a, %d %b %Y %X %Z")

def get(sess, url, mediatype, params={}, **kwargs):
    # TODO: warn on 301 redirect? https://docs.github.com/en/rest/using-the-rest-api/best-practices-for-using-the-rest-api?apiVersion=2022-11-28#follow-redirects

    while True:
        print(url, end="", flush=True)
        try:
            headers = {}
            if mediatype is not None:
                headers["Accept"] = mediatype
            r = sess.get(url, params=params, headers=headers, **kwargs)
        except Exception as e:
            print(f" => {str(type(e))}", flush=True)
            raise

        print(f" => {r.status_code} {r.reason} {r.headers.get('x-ratelimit-used', '-')}/{r.headers.get('x-ratelimit-limit', '-')}", flush=True)
        reset = rate_limit_reset(r)
        if reset is not None:
            reset_seconds = (reset - response_datetime(r)).total_seconds()
            print(f"waiting {reset_seconds:.0f} s for rate limit, will resume at {reset.strftime('%Y-%m-%d %H:%M:%S')}", flush=True)
            time.sleep(reset_seconds)
        else:
            r.raise_for_status()
            return r

# https://docs.github.com/en/rest/using-the-rest-api/using-pagination-in-the-rest-api?apiVersion=2022-11-28
def get_paginated(sess, url, mediatype, params={}, **kwargs):
    params = params.copy()
    try:
        del params["page"]
    except KeyError:
        pass
    params["per_page"] = "100"

    while True:
        r = get(sess, url, mediatype, params, **kwargs)
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

def get_to_tempfile(sess, url, mediatype, params={}):
    r = get(sess, url, mediatype, params, stream=True)
    tmp = tempfile.TemporaryFile()
    for chunk in r.iter_content(4096):
        tmp.write(chunk)
    return tmp

# Fallback to mistune 1.0 renderer if mistune 2.0 is not installed
try:
    mistuneRenderer = mistune.HTMLRenderer
except AttributeError:
    mistuneRenderer = mistune.Renderer
# Custom mistune.Renderer that stores a list of all links encountered.
class LinkExtractionRenderer(mistuneRenderer):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.links = []

    def autolink(self, link, is_email=False):
        self.links.append(link)
        return super().autolink(link, is_email)

    def image(self, src, title, alt_text):
        self.links.append(src)
        return super().image(src, title, alt_text)

    def link(self, link, title, content=None):
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

# Return True or False, according to whether url is one we want to download.
def link_is_wanted(url):
    try:
        components = urllib.parse.urlparse(url)
    except ValueError:
        return False

    if components.scheme == "https" and components.netloc == "user-images.githubusercontent.com":
        subpath = strip_url_path_prefix(components.path, "")
        if subpath is not None:
            # Inline image.
            return True
    if components.scheme == "https" and components.netloc == "github.com":
        for prefix in (f"/{owner}/{repo}/files", "/user-attachments/files"):
            subpath = strip_url_path_prefix(components.path, prefix)
            if subpath is not None:
                # File attachment.
                return True
    if components.scheme == "https" and components.netloc == "avatars.githubusercontent.com":
        path = components.path
        if components.query:
            # Avatar URLs often differ in the presence or absence of query
            # parameters. Save the query string with the path, just in case they
            # differ.
            path += "?" + components.query
        subpath = strip_url_path_prefix(path, "")
        if subpath is not None:
            # Avatar image.
            return True

    return False

def backup(owner, repo, db, username, token):
    # Escape owner and repo suitably for use in a URL.
    owner = urllib.parse.quote(owner, safe="")
    repo = urllib.parse.quote(repo, safe="")

    sess = requests.Session()

    if (username is None) != (token is None):
        # Both must be None or both non-None.
        raise ValueError("only one of username and token was supplied")
    if username is not None and token is not None:
        # HTTP Basic authentication for API.
        sess.auth = requests.auth.HTTPBasicAuth(username, token)

    # https://docs.github.com/en/rest/issues/issues?apiVersion=2022-11-28#list-repository-issues
    issues_url = urllib.parse.urlparse(BASE_URL)._replace(
        path=f"/repos/{owner}/{repo}/issues",
    ).geturl()
    for r in get_paginated(sess, issues_url, MEDIATYPE, {"state": "all", "sort": "created", "direction": "asc"}):
        for issue in r.json():
            if db.execute("SELECT NULL FROM issues WHERE id = :id", {"id": issue["id"]}).fetchone():
                # Already fetched this issue.
                continue
            # Fetch the JSON content of the issue URL.
            check_url_origin(BASE_URL, issue["url"])
            with get_to_tempfile(sess, issue["url"], MEDIATYPE) as body:
                db.execute("BEGIN")
                with db:
                    # Insert a database row with a placeholder json blob.
                    rowid = db.execute("INSERT INTO issues VALUES (:id, zeroblob(:blob_size))", {
                        "id": issue["id"],
                        "blob_size": body.tell(),
                    }).lastrowid
                    # Write the response body into the json blob.
                    with db.blobopen("issues", "json", rowid) as blob:
                        body.seek(0)
                        shutil.copyfileobj(body, blob)
                        assert body.tell() == len(blob), (body.tell(), len(blob))

                    # There's no API for getting all reactions in a repository,
                    # so get them per issue and per comment.
                    # https://docs.github.com/en/rest/reactions/reactions?apiVersion=2022-11-28#list-reactions-for-an-issue
                    if issue["reactions"]["total_count"] != 0:
                        check_url_origin(BASE_URL, issue["reactions"]["url"])
                        for r2 in get_paginated(sess, issue["reactions"]["url"], MEDIATYPE):
                            for reaction in r2.json():
                                db.execute("INSERT into issue_reactions VALUES (:id, :issue_id, :json)", {
                                    "id": reaction["id"],
                                    "issue_id": issue["id"],
                                    "json": json.dumps(reaction).encode("utf-8"),
                                })

    # https://docs.github.com/en/rest/issues/comments?apiVersion=2022-11-28#list-issue-comments
    # Comments are linked to their parent issue via the issue_url field.
    comments_url = urllib.parse.urlparse(BASE_URL)._replace(
        path=f"/repos/{owner}/{repo}/issues/comments",
    ).geturl()
    for r in get_paginated(sess, comments_url, MEDIATYPE):
        for comment in r.json():
            if db.execute("SELECT NULL FROM comments WHERE id = :id", {"id": comment["id"]}).fetchone():
                # Already fetched this comment.
                continue
            with get_to_tempfile(sess, comment["url"], MEDIATYPE) as body:
                db.execute("BEGIN")
                with db:
                    # Insert a database row with a placeholder json blob.
                    rowid = db.execute("INSERT INTO comments VALUES (:id, zeroblob(:blob_size))", {
                        "id": comment["id"],
                        "blob_size": body.tell(),
                    }).lastrowid
                    # Write the response body into the json blob.
                    with db.blobopen("comments", "json", rowid) as blob:
                        body.seek(0)
                        shutil.copyfileobj(body, blob)
                        assert body.tell() == len(blob), (body.tell(), len(blob))

                    # There's no API for getting all reactions in a repository,
                    # so get them per issue and per comment.
                    # https://docs.github.com/en/rest/reactions/reactions?apiVersion=2022-11-28#list-reactions-for-an-issue
                    if comment["reactions"]["total_count"] != 0:
                        check_url_origin(BASE_URL, comment["reactions"]["url"])
                        for r2 in get_paginated(sess, comment["reactions"]["url"], MEDIATYPE):
                            for reaction in r2.json():
                                db.execute("INSERT into comment_reactions VALUES (:id, :comment_id, :json)", {
                                    "id": reaction["id"],
                                    "comment_id": comment["id"],
                                    "json": json.dumps(reaction).encode("utf-8"),
                                })

            # TODO: comment edit history (if possible)

    # https://docs.github.com/en/rest/issues/labels?apiVersion=2022-11-28#list-labels-for-a-repository
    labels_url = urllib.parse.urlparse(BASE_URL)._replace(
        path=f"/repos/{owner}/{repo}/labels",
    ).geturl()
    for r in get_paginated(sess, labels_url, MEDIATYPE):
        for label in r.json():
            if db.execute("SELECT NULL FROM labels WHERE id = :id", {"id": label["id"]}).fetchone():
                # Already fetched this label.
                continue
            with get_to_tempfile(sess, label["url"], MEDIATYPE) as body:
                db.execute("BEGIN")
                with db:
                    # Insert a database row with a placeholder json blob.
                    rowid = db.execute("INSERT INTO labels VALUES (:id, zeroblob(:blob_size))", {
                        "id": label["id"],
                        "blob_size": body.tell(),
                    }).lastrowid
                    # Write the response body into the json blob.
                    with db.blobopen("labels", "json", rowid) as blob:
                        body.seek(0)
                        shutil.copyfileobj(body, blob)
                        assert body.tell() == len(blob), (body.tell(), len(blob))

    # A new session, without Basic auth, for downloading plain files.
    sess = requests.Session()

    # Parse issue and comment text for links and avatar URLs.
    def all_json():
        for (blob,) in db.execute("SELECT json FROM issues"):
            yield blob
        for (blob,) in db.execute("SELECT json FROM comments"):
            yield blob
    def scrape_links():
        for blob in all_json():
            data = json.loads(blob)
            yield data["user"]["avatar_url"]
            for link in markdown_extract_links(data["body"] or ""):
                yield urllib.parse.urlunparse(urllib.parse.urlparse(link)._replace(fragment = None)) # Discard fragment.
    seen = set(url for (url,) in db.execute("SELECT url FROM files"))
    for link in scrape_links():
        if not link_is_wanted(link):
            continue
        if link in seen:
            continue
        seen.add(link)
        with get_to_tempfile(sess, link, MEDIATYPE) as body:
            db.execute("BEGIN")
            with db:
                # Insert a database row with a placeholder json blob.
                rowid = db.execute("INSERT INTO files VALUES (:url, zeroblob(:blob_size))", {
                    "url": link,
                    "blob_size": body.tell(),
                }).lastrowid
                # Write the response body into the data blob.
                with db.blobopen("files", "data", rowid) as blob:
                    body.seek(0)
                    shutil.copyfileobj(body, blob)
                    assert body.tell() == len(blob), (body.tell(), len(blob))

def create_tables(db):
    db.execute("BEGIN")
    with db:
        # Make id columns UNIQUE (so they may be the parent of a foreign key
        # constraint), but not PRIMARY KEY. An INTEGER PRIMARY KEY takes over
        # the role of the rowid for the table, and rowid values that are larger
        # than 31 bits crash the blobopen function in certain versions of Python.
        # https://github.com/python/cpython/issues/100370
        db.execute("""\
CREATE TABLE IF NOT EXISTS issues (
    id INTEGER UNIQUE,
    json BLOB
) STRICT""")
        db.execute("""\
CREATE TABLE IF NOT EXISTS comments (
    id INTEGER UNIQUE,
    json BLOB
) STRICT""")
        db.execute("""\
CREATE TABLE IF NOT EXISTS labels (
    id INTEGER UNIQUE,
    json BLOB
) STRICT""")
        db.execute("""\
CREATE TABLE IF NOT EXISTS issue_reactions (
    id INTEGER UNIQUE,
    issue_id INTEGER,
    json BLOB,
    FOREIGN KEY(issue_id) REFERENCES issues(id)
) STRICT""")
        db.execute("""\
CREATE TABLE IF NOT EXISTS comment_reactions (
    id INTEGER UNIQUE,
    comment_id INTEGER,
    json BLOB,
    FOREIGN KEY(comment_id) REFERENCES comments(id)
) STRICT""")
        db.execute("""\
CREATE TABLE IF NOT EXISTS files (
    url TEXT,
    data BLOB
) STRICT""")

# Escape a filename so that sqlite3 will interpret it as a filesystem path, not
# as a special filename such as ":memory:" or a URI filename beginning with
# "file:".
def sqlite_escape_filename(filename):
    # The three cases which sqlite3 may interpret specially are: path begins
    # with ":" (e.g. ":memory:"), path begins with "file:" (URI), or path is
    # empty (which means to use a temporary file as the backing store). If path
    # is absolute, none of these are possible, so return the original path
    # unmodified. Otherwise, simply prepend a "./" to defuse all of these.
    if os.path.isabs(filename):
        return filename
    else:
        return os.path.join(".", filename)

if __name__ == "__main__":
    username = None
    token = None

    opts, (repo, db_filename) = getopt.gnu_getopt(sys.argv[1:], "u:")
    for o, a in opts:
        if o == "-u":
            username, token = a.split(":", 1)
        elif o in ("-h", "--help"):
            pass

    owner, repo = repo.split("/", 1)

    db = sqlite3.connect(sqlite_escape_filename(db_filename), isolation_level=None)
    try:
        db.execute("PRAGMA foreign_keys = ON")
        create_tables(db)
        backup(owner, repo, db, username, token)
        db.execute("VACUUM")
    finally:
        db.close()
