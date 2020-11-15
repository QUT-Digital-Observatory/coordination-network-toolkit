import urllib3
import requests
import sqlite3 as lite
from concurrent.futures import ThreadPoolExecutor, wait
import time
from logging import getLogger


logger = getLogger(__name__)


# Disable the warnings: we're going to try validating certs first, falling
# back to unverified second, setting a flag if it couldn't be verified.
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def resolve_url(
    url, timeout=(15.05, 15), from_header=None, max_redirects=5, verify_ssl=True
):
    """
    Follow redirects of a URL to the end of the chain.

    The final URL in the chain (regardless of the status code at that point) is the
    'resolved' URL.

    Parameters:

    url: the URL to follow.

    timeout: the timeout to consider before marking the connection as failed.

    from_header: an optional email address to add to the from header, so you can be a
        good netizen.

    max_redirects: the number of redirects to take before stopping.

    Returns:

        (url, resolved_url, final_status_code)

    In the case that the request times out, or the max number of redirects is
    reached, that will be recorded as a string error 'TIMEOUT' or
    'TOO_MANY_REDIRECTS'.

    An attempt will be made to verify the certificates of the redirects: if
    an SSL error is raised SSLError will be reported as the status.

    """

    # Create a requests session to set the max_redirects, and make sure to include a
    # from header in the requests so we are contactable if we're causing problems.
    session = requests.Session()

    if from_header:
        session.headers.update({"From": from_header})

    # The default is 30, we're reducing it a lot here, to avoid hammering servers in
    # the case of a redirect loop.
    session.max_redirects = max_redirects

    # If we cannot resolve the URL, it will be left at the original URL.
    resolved_url = url

    try:
        r = session.head(url, allow_redirects=True, timeout=timeout)
        status_code = r.status_code
    except requests.exceptions.SSLError as e:
        if verify_ssl:
            return resolve_url(
                url,
                timeout=timeout,
                from_header=from_header,
                max_redirects=max_redirects,
                verify_ssl=False,
            )
        else:
            status_code = "SSLError"
            r = e.response
    except requests.Timeout as e:
        status_code = "Timeout"
        r = e.response
    except requests.TooManyRedirects as e:
        status_code = "TooManyRedirects"
        r = e.response
    except requests.ConnectionError as e:
        status_code = "ConnectionError"
        r = e.response
    except Exception as e:
        status_code = "OtherError"
        r = None
        logger.exception(e)

    if r is not None:
        resolved_url = r.url

    return url, resolved_url, verify_ssl, status_code


def resolve_all_urls(db_path, max_redirects):
    """
    Resolve all unresolved URLs in the given database.

    Resolution is rate limited to 25 URLs per second.

    The database itself is used as the queue for managing what work needs to be done.

    :param max_redirects: The maximum number of redirects to allow before terminating
    resolution.

    """
    db = lite.connect(db_path)

    pool = ThreadPoolExecutor(50)
    # While it would be nice to not have to track the futures ourselves, the
    # alternative would be to either manage a separate queue of URLs to process
    # or just to hand over every single URL to the pool on startup.
    future_list = set()

    with db:
        to_resolve = [
            row[0]
            for row in db.execute(
                "select url from resolved_url where resolved_url is null",
            )
        ]

    n_urls_to_pause = 25

    n_to_resolve = len(to_resolve)
    n_resolved = 0

    print(f"Resolving {n_to_resolve} URLs")

    for url in to_resolve:

        future = pool.submit(resolve_url, url, max_redirects=max_redirects)
        future_list.add(future)
        n_urls_to_pause -= 1

        # This is the rate limiting step - we only add to the queue 125 URLs
        # at a time, then we pause to prevent too much concurrency.
        # This means the maximum rate at which we can resolve urls is 25/second.
        if n_urls_to_pause == 0:
            n_urls_to_pause = 25
            time.sleep(1)

        # When the inflight is too big, move onto the next one
        if len(future_list) >= 1000:
            completed, not_completed = wait(future_list, timeout=0)
            future_list = not_completed

            with db:
                for f in completed:
                    row = f.result()
                    db.execute("replace into resolved_url values(?, ?, ?, ?)", row)

                    n_resolved += 1
            print(f"Resolved {n_resolved} of {n_to_resolve} URLs")

    else:
        # Wait for everything to be done, before starting again
        completed, _ = wait(future_list)

        with db:
            for f in completed:
                row = f.result()
                db.execute("replace into resolved_url values(?, ?, ?, ?)", row)

    print("Constructing new resolved url tables")

    with db:
        db.executescript(
            """
            drop table if exists resolved_message_url;

            create table resolved_message_url(
                message_id references edge(message_id),
                resolved_url,
                timestamp,
                user_id,
                primary key (message_id, resolved_url)
            );

            insert or ignore into resolved_message_url
            select 
                message_id,
                resolved_url.resolved_url,
                timestamp,
                user_id
            from message_url
            inner join resolved_url using(url)
            """
        )
