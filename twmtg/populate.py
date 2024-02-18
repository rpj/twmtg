import asyncio
import functools
import hashlib
import re
import subprocess
from pathlib import Path
from typing import Optional

import aiosqlite
import httpx
from alive_progress import alive_bar
from rich import print

TWENTY = 20

MTGJSON_SQLITE_CHECKSUM_URL = (
    "https://mtgjson.com/api/v5/AllPrintings.sqlite.bz2.sha256"
)
MTGJSON_SQLITE_ASSET_URL = "https://mtgjson.com/api/v5/AllPrintings.sqlite.bz2"


async def http_get_path_cached_checksummed(asset_url: str, checksum_url: str) -> Path:
    async with httpx.AsyncClient() as client:

        async def _get_current_checksum():
            cs_res = await client.get(checksum_url)
            if cs_res.status_code != 200:
                raise Exception(
                    f"http_get_checksummed checksum {checksum_url}: {cs_res.status_code}"
                )
            return cs_res.text

        comb_shasum = hashlib.sha224(
            asset_url.encode("utf-8") + checksum_url.encode("utf-8")
        ).hexdigest()
        parent = Path(__file__).resolve().parent
        combsum_file = parent / f".{comb_shasum}.checksum"
        asset_file = parent / f".{comb_shasum}"

        async def _asset_needs_refresh() -> Optional[str]:
            """
            returning 'None' means: refresh not needed
            otherwise it's the fetched checksum of the new asset
            """
            fetched_checksum = await _get_current_checksum()
            if asset_file.exists() and combsum_file.exists():
                with open(combsum_file, "r") as cs_f:
                    current_checksum = cs_f.read()
                    if fetched_checksum == current_checksum:
                        return None
            return fetched_checksum

        nr_checksum = await _asset_needs_refresh()
        if nr_checksum:
            print(f"MTGJSON source data needs to be fetched...")
            with open(combsum_file, "w+") as cs_w:
                cs_w.write(nr_checksum)

            with open(asset_file, "wb+") as as_w:
                async with client.stream("GET", asset_url) as stream:
                    async for chunk in stream.aiter_bytes():
                        as_w.write(chunk)

        return asset_file


async def mtgjson_sqlite_path():
    asset_path = await http_get_path_cached_checksummed(
        MTGJSON_SQLITE_ASSET_URL, MTGJSON_SQLITE_CHECKSUM_URL
    )
    asset_uncompressed = Path(str(asset_path) + ".out")

    if not asset_uncompressed.exists():
        print("Uncompressing...")
        process = subprocess.run(["bunzip2", "-k", asset_path])
        if process.returncode != 0:
            raise Exception("bunzip2")

    return asset_uncompressed


FILTER_STRINGS = ["This spell costs {1} more to cast for each target beyond the first."]


async def main():
    asset_uncompressed = await mtgjson_sqlite_path()
    async with aiosqlite.connect(asset_uncompressed) as db:
        await db.execute(
            (
                "create table if not exists twentyword_cards ("
                "card_uuid VARCHAR(36) NOT NULL PRIMARY KEY,"
                "legal BOOLEAN NOT NULL,"
                "legality_checked_text text not null,"
                "num_words INTEGER NOT NULL"
                ")"
            )
        )
        await db.commit()

        [(count,)] = await db.execute_fetchall("select count(*) from cards")
        print(f"Processing legality of {count} cards...")
        with alive_bar(count) as bar:
            cursor = await db.execute(
                "select uuid, replace(text, '\\n', ' ') as text from cards"
            )
            await db.commit()
            async for row in cursor:
                (uuid, text) = row
                if text:
                    text_rm_reminder_text = re.sub(
                        r"\s+", " ", re.sub(r"\([^\)]+\)", "", text).strip()
                    )
                    text_filtered = functools.reduce(
                        lambda t, fs: t.replace(fs, ""),
                        FILTER_STRINGS,
                        text_rm_reminder_text,
                    ).strip()
                    num_words = len(text_filtered.split(" "))
                    legal = num_words <= TWENTY
                    await db.execute(
                        "insert into twentyword_cards values (?, ?, ?, ?) "
                        "on conflict (card_uuid) do update set legal = excluded.legal, "
                        "legality_checked_text = excluded.legality_checked_text, "
                        "num_words = excluded.num_words",
                        (uuid, legal, text_filtered, num_words),
                    )
                    await db.commit()
                bar()

        [(legal_count,)] = await db.execute_fetchall(
            "select count(*) from cards left join twentyword_cards "
            + "where cards.uuid = twentyword_cards.card_uuid and twentyword_cards.legal = 1"
        )
        [(illegal_count,)] = await db.execute_fetchall(
            "select count(*) from cards left join twentyword_cards "
            + "where cards.uuid = twentyword_cards.card_uuid and twentyword_cards.legal = 0"
        )
        print(f"{legal_count} legal, {illegal_count} illegal")


if __name__ == "__main__":
    asyncio.run(main())
