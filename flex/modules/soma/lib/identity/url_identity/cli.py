#!/usr/bin/env python3
"""
URL Identity CLI

Usage:
    urlid assign <url>              # register URL, return url_id
    urlid resolve <url>             # get url_id without creating
    urlid get <url_id>              # full info
    urlid locate <url_id>           # get canonical URL
    urlid fetches <url_id>          # fetch history
    urlid content <url_id>          # get latest content
    urlid drift <url_id>            # check drift status
    urlid list [--domain D]         # list URLs
    urlid stats                     # storage stats
    urlid normalize <url>           # show normalized form
    urlid serve [--port N]          # local HTTP server to view cached pages
"""

import argparse
import sys
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

from .identity import URLIdentity


def cmd_assign(args):
    ui = URLIdentity()
    url_id = ui.assign(args.url, is_search=args.search)
    print(url_id)


def cmd_resolve(args):
    ui = URLIdentity()
    url_id = ui.resolve(args.url, is_search=args.search)
    if url_id:
        print(url_id)
    else:
        print(f"URL not tracked: {args.url}", file=sys.stderr)
        sys.exit(1)


def cmd_get(args):
    ui = URLIdentity()
    info = ui.get(args.url_id)
    if not info:
        print(f"url_id not found: {args.url_id}", file=sys.stderr)
        sys.exit(1)

    drift_status = "DRIFTED" if info.drift_detected else "stable"

    print(f"url_id:    {info.url_id}")
    print(f"canonical: {info.canonical_url}")
    if info.original_url:
        print(f"original:  {info.original_url}")
    print(f"scheme:    {info.scheme}")
    print(f"domain:    {info.domain or 'N/A'}")
    print(f"first_seen: {info.first_seen}")
    print(f"last_fetch: {info.last_fetched or 'never'}")
    print(f"fetches:   {info.fetch_count}")
    print(f"drift:     {drift_status}")


def cmd_locate(args):
    ui = URLIdentity()
    url = ui.locate(args.url_id)
    if url:
        print(url)
    else:
        print(f"url_id not found: {args.url_id}", file=sys.stderr)
        sys.exit(1)


def cmd_fetches(args):
    ui = URLIdentity()
    fetches = ui.get_fetches(args.url_id, limit=args.limit)

    if not fetches:
        print(f"No fetches for url_id: {args.url_id}", file=sys.stderr)
        sys.exit(1)

    print(f"Fetch history for {args.url_id[:8]}...\n")
    for f in fetches:
        hash_short = f.content_hash[:12] + "..." if f.content_hash else "N/A"
        status = f.status_code or "?"
        size = f"{f.response_size:,}" if f.response_size else "?"
        ts = f.fetched_at[:16]
        print(f"  {ts}  {status}  {size:>10}B  {hash_short}")

    print(f"\n{len(fetches)} fetch(es)")


def cmd_content(args):
    ui = URLIdentity()
    content = ui.get_content(args.url_id, at_time=args.at)

    if not content:
        print(f"No content for url_id: {args.url_id}", file=sys.stderr)
        sys.exit(1)

    # Output raw content
    if args.raw:
        sys.stdout.buffer.write(content)
    else:
        try:
            print(content.decode('utf-8'))
        except UnicodeDecodeError:
            print(f"Binary content ({len(content)} bytes)", file=sys.stderr)
            sys.exit(1)


def cmd_drift(args):
    ui = URLIdentity()

    if not ui.exists(args.url_id):
        print(f"url_id not found: {args.url_id}", file=sys.stderr)
        sys.exit(1)

    if ui.has_drifted(args.url_id):
        print("DRIFTED")
        history = ui.get_drift_history(args.url_id)
        if history:
            print(f"\n{len(history)} content change(s):\n")
            for h in history:
                print(f"  {h['changed_at'][:16]}")
                print(f"    from: {h['from_hash'][:12]}...")
                print(f"    to:   {h['to_hash'][:12]}...")
        sys.exit(1)
    else:
        print("STABLE")


def cmd_list(args):
    ui = URLIdentity()

    if args.domain:
        urls = ui.list_by_domain(args.domain, limit=args.limit)
    elif args.drifted:
        urls = ui.list_drifted(limit=args.limit)
    else:
        urls = ui.list_recent(limit=args.limit)

    if not urls:
        print("No URLs tracked")
        return

    for u in urls:
        drift = "!" if u.drift_detected else " "
        fetches = f"[{u.fetch_count}]" if u.fetch_count else ""
        print(f"{drift} {u.url_id[:8]}  {fetches:>5}  {u.canonical_url[:80]}")

    print(f"\n{len(urls)} URL(s)")


def cmd_stats(args):
    ui = URLIdentity()
    s = ui.stats()

    print(f"URLs:      {s['url_count']:,}")
    print(f"Fetches:   {s['fetch_count']:,}")
    print(f"Domains:   {s['domains']:,}")
    print(f"Drifted:   {s['drifted_count']:,}")
    print(f"Content:   {s['content_size_mb']:.2f} MB")


def cmd_normalize(args):
    ui = URLIdentity()
    if args.search:
        normalized = ui.normalize_search_query(args.url)
    else:
        normalized = ui.normalize(args.url)
    print(normalized)


def cmd_serve(args):
    """Serve cached web pages via local HTTP server."""
    ui = URLIdentity()

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, format, *args):
            # Quiet logging
            pass

        def do_GET(self):
            parsed = urlparse(self.path)
            path = parsed.path.strip('/')

            # Index page - list all URLs
            if not path or path == 'index':
                self.send_response(200)
                self.send_header('Content-Type', 'text/html')
                self.end_headers()

                urls = ui.list_recent(limit=100)
                html = ['<html><head><title>URL Archive</title>',
                        '<style>body{font-family:monospace;padding:20px}',
                        'a{color:#0066cc}tr:hover{background:#f0f0f0}</style></head>',
                        '<body><h1>URL Archive</h1>',
                        f'<p>{len(urls)} cached pages</p>',
                        '<table><tr><th>ID</th><th>URL</th><th>Fetches</th><th>Drift</th></tr>']

                for u in urls:
                    drift = '!' if u.drift_detected else ''
                    html.append(f'<tr><td><a href="/{u.url_id}">{u.url_id[:8]}</a></td>')
                    html.append(f'<td>{u.canonical_url[:80]}</td>')
                    html.append(f'<td>{u.fetch_count}</td><td>{drift}</td></tr>')

                html.append('</table></body></html>')
                self.wfile.write('\n'.join(html).encode())
                return

            # Serve content by url_id
            url_id = path

            # Check for version query param (?v=N for Nth fetch)
            query = parse_qs(parsed.query)
            version = query.get('v', [None])[0]

            content = None
            if version:
                # Get specific fetch version
                fetches = ui.get_fetches(url_id, limit=100)
                try:
                    idx = int(version) - 1
                    if 0 <= idx < len(fetches):
                        fetch = fetches[idx]
                        if fetch.content_hash and ui.content:
                            content = ui.content.retrieve(fetch.content_hash)
                except (ValueError, IndexError):
                    pass
            else:
                content = ui.get_content(url_id)

            if content:
                self.send_response(200)
                # Guess content type
                if content[:5] == b'<?xml' or content[:5] == b'<html' or b'<!DOCTYPE' in content[:50]:
                    self.send_header('Content-Type', 'text/html; charset=utf-8')
                else:
                    self.send_header('Content-Type', 'text/plain; charset=utf-8')
                self.end_headers()
                self.wfile.write(content)
            else:
                self.send_response(404)
                self.send_header('Content-Type', 'text/plain')
                self.end_headers()
                info = ui.get(url_id)
                if info:
                    self.wfile.write(f'No content cached for: {info.canonical_url}'.encode())
                else:
                    self.wfile.write(f'Unknown url_id: {url_id}'.encode())

    port = args.port
    server = HTTPServer(('127.0.0.1', port), Handler)
    print(f'Serving URL archive at http://127.0.0.1:{port}')
    print(f'  Index:   http://127.0.0.1:{port}/')
    print(f'  Page:    http://127.0.0.1:{port}/<url_id>')
    print(f'  Version: http://127.0.0.1:{port}/<url_id>?v=1')
    print('Press Ctrl+C to stop')

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print('\nStopped')


def main():
    parser = argparse.ArgumentParser(description="URL Identity System")
    subs = parser.add_subparsers(dest="cmd", required=True)

    # assign
    p = subs.add_parser("assign", help="Register URL, return url_id")
    p.add_argument("url")
    p.add_argument("-s", "--search", action="store_true", help="Treat as search query")
    p.set_defaults(func=cmd_assign)

    # resolve
    p = subs.add_parser("resolve", help="Get url_id without creating")
    p.add_argument("url")
    p.add_argument("-s", "--search", action="store_true", help="Treat as search query")
    p.set_defaults(func=cmd_resolve)

    # get
    p = subs.add_parser("get", help="Full info for URL")
    p.add_argument("url_id")
    p.set_defaults(func=cmd_get)

    # locate
    p = subs.add_parser("locate", help="Get canonical URL for url_id")
    p.add_argument("url_id")
    p.set_defaults(func=cmd_locate)

    # fetches
    p = subs.add_parser("fetches", help="Fetch history for URL")
    p.add_argument("url_id")
    p.add_argument("-n", "--limit", type=int, default=20)
    p.set_defaults(func=cmd_fetches)

    # content
    p = subs.add_parser("content", help="Get content for URL")
    p.add_argument("url_id")
    p.add_argument("--at", help="Get content as of ISO timestamp")
    p.add_argument("--raw", action="store_true", help="Output raw bytes")
    p.set_defaults(func=cmd_content)

    # drift
    p = subs.add_parser("drift", help="Check drift status")
    p.add_argument("url_id")
    p.set_defaults(func=cmd_drift)

    # list
    p = subs.add_parser("list", help="List tracked URLs")
    p.add_argument("-d", "--domain", help="Filter by domain")
    p.add_argument("--drifted", action="store_true", help="Show only drifted URLs")
    p.add_argument("-n", "--limit", type=int, default=50)
    p.set_defaults(func=cmd_list)

    # stats
    p = subs.add_parser("stats", help="Storage statistics")
    p.set_defaults(func=cmd_stats)

    # normalize
    p = subs.add_parser("normalize", help="Show normalized form of URL")
    p.add_argument("url")
    p.add_argument("-s", "--search", action="store_true", help="Normalize as search query")
    p.set_defaults(func=cmd_normalize)

    # serve
    p = subs.add_parser("serve", help="Local HTTP server to view cached pages")
    p.add_argument("-p", "--port", type=int, default=8888, help="Port (default: 8888)")
    p.set_defaults(func=cmd_serve)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
