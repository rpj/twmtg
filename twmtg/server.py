import argparse
import asyncio
from collections import defaultdict

import aiohttp_cors
import aiosqlite
import chevron
from aiohttp import web
from rich import print

from twmtg.populate import mtgjson_sqlite_path

DEFAULT_PORT = 4212
TWMTG_RULESET_VER = "20240217"


class TWMTGHTTPAPI:  # ACRONYM SOUP!!!!!!
    port: int
    app: web.Application

    def __init__(
        self,
        port: int = DEFAULT_PORT,
    ):
        self.port = port

        self.app = web.Application()
        self.app_cors = aiohttp_cors.setup(
            self.app,
            defaults={
                "*": aiohttp_cors.ResourceOptions(
                    allow_credentials=True, expose_headers="*", allow_headers="*"
                )
            },
        )

        cors_allowed_routes = [
            web.get("/card", self.twentywordmagic_cards),
            web.get("/count", self.twentywordmagic_count),
            web.get("/meta", self.twentywordmagic_meta),
        ]

        self.app.add_routes([*cors_allowed_routes])

        cors_allowed_paths = [route.path for route in cors_allowed_routes]
        for route in list(self.app.router.routes()):
            route_info = route.resource.get_info()
            if "path" in route_info and route_info["path"] in cors_allowed_paths:
                print(f"Opening up CORS on {route}")
                self.app_cors.add(route)

        self.mtgjson_db_path = None
        self.runner = web.AppRunner(self.app, handle_signals=False)

    async def setup(self):
        # do this here because mtgjson_sqlite_path() makes a request every call (at least one) to mtgjson.com
        # and also it can block for a LONG time (really should be refactored!)
        self.mtgjson_db_path = await mtgjson_sqlite_path()
        await self.runner.setup()

    async def serve(self):
        print(f"HTTP API listening on port {self.port}")
        site = web.TCPSite(self.runner, port=self.port)
        await site.start()
        await asyncio.Event().wait()

    async def shutdown(self):
        await self.app.shutdown()
        await self.app.cleanup()

    async def twentywordmagic_meta(self, req: web.Request):
        async with aiosqlite.connect(self.mtgjson_db_path) as db:
            [(date, ver)] = await db.execute_fetchall("select * from meta")
            with open("templates/twmtg_card_meta_frag.html", "r") as frag:
                return web.Response(
                    text=chevron.render(
                        frag.read(),
                        {
                            "ruleset_ver": TWMTG_RULESET_VER,
                            "mtgjson_date": date,
                            "mtgjson_ver": ver,
                        },
                    ),
                    content_type="text/html",
                )

    async def twentywordmagic_count(self, req: web.Request):
        async with aiosqlite.connect(self.mtgjson_db_path) as db:

            async def _count(legal=True):
                [(count,)] = await db.execute_fetchall(
                    "select count(*) from cards left join twentyword_cards "
                    + "where cards.uuid = twentyword_cards.card_uuid and twentyword_cards.legal = ?",
                    ("1" if legal else "0"),
                )
                await db.commit()
                return count

            count = None
            if "total" in req.query or "illegal-pct" in req.query:
                legal = await _count()
                illegal = await _count(False)
                count = legal + illegal
                if "illegal-pct" in req.query:
                    return web.Response(
                        text=f"{(float(illegal) / float(count)) * 100:.0f}%",
                        content_type="text/html",
                    )
            else:
                count = await _count(False if "illegal" in req.query else True)
            return web.Response(text=f"{count:,}", content_type="text/html")

    async def twentywordmagic_cards(self, req: web.Request):
        if "card_name" not in req.query:
            return
        async with aiosqlite.connect(self.mtgjson_db_path) as db:
            db.row_factory = aiosqlite.Row
            records = await db.execute_fetchall(
                "select * from cards left join twentyword_cards as tw "
                + "left join cardPurchaseUrls as urls "
                + "where cards.uuid = tw.card_uuid and cards.uuid = urls.uuid and cards.name like ?",
                (req.query["card_name"],),
            )
            await db.commit()

            if len(records) == 0:
                with open("templates/twmtg_card_not_found_frag.html", "r") as frag:
                    frag_str = frag.read()
                    return web.Response(
                        text=chevron.render(
                            frag_str, {"card_name": req.query["card_name"]}
                        ),
                        content_type="text/html",
                    )

            html_out = ""
            frag_str = None
            with open("templates/twmtg_card_frag.html", "r") as frag:
                frag_str = frag.read()

            unique_texts = defaultdict(list)
            for row in records:
                row_dict = {**row}
                if "text" in row_dict and len(row_dict["text"]):
                    unique_texts[row_dict["text"]].append(row_dict)

            for same_text_rows in unique_texts.values():
                row_copy = {**same_text_rows[0]}
                row_copy["tcgplayer_links"] = list(
                    filter(
                        lambda r: r["link"] is not None,
                        [
                            {"set": row["setCode"], "link": row["tcgplayer"]}
                            for row in same_text_rows
                        ],
                    )
                )
                row_copy["text"] = row_copy["text"].replace("\\n", "<br/>")
                if row_copy["legal"]:
                    row_copy["TMPL_legal"] = [True]
                html_out += chevron.render(frag_str, row_copy)
                html_out += "\n"

            return web.Response(text=html_out, content_type="text/html")


def parse_args() -> argparse.Namespace:
    args = argparse.ArgumentParser(description="")
    args.add_argument(
        "--port",
        type=int,
        default=DEFAULT_PORT,
        help=f"Serving port, default: {DEFAULT_PORT}",
    )
    return args.parse_args()


async def main():
    args = parse_args()
    app = TWMTGHTTPAPI(port=args.port)
    await app.setup()
    await app.serve()


if __name__ == "__main__":
    asyncio.run(main())
