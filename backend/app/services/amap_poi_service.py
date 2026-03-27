"""AMAP (高德) POI search service.

Wraps the AMAP Web Service API for text search and around search.
Used by daily_runner to populate the POI cache (replacing XHS crawler).
"""

import asyncio
import logging
from typing import Any

import httpx

from app.pipeline.amap_config import AMAP_TYPE_MAPPING

logger = logging.getLogger(__name__)

_TEXT_URL = "https://restapi.amap.com/v3/place/text"
_AROUND_URL = "https://restapi.amap.com/v3/place/around"


class AmapPoiService:
    """AMAP (高德) POI search service."""

    def __init__(self, api_key: str = "") -> None:
        self.api_key = api_key
        self._client: httpx.AsyncClient | None = None

    async def _get_client(self) -> httpx.AsyncClient:
        if self._client is None:
            self._client = httpx.AsyncClient(timeout=httpx.Timeout(10.0))
        return self._client

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def search_text(
        self,
        city: str,
        types: str = "",
        keywords: str = "",
        page: int = 1,
        offset: int = 25,
    ) -> list[dict[str, Any]]:
        """Keyword + type search within a city.

        Args:
            city: City name (e.g. "苏州").
            types: AMAP type codes (e.g. "050000", "110000|110100").
            keywords: Optional keyword filter.
            page: Page number (1-indexed).
            offset: Results per page (max 25).

        Returns:
            Parsed POI dicts.
        """
        if not self.api_key:
            logger.warning("No AMAP API key, returning mock data")
            return self._mock_search(city, types)

        params: dict[str, Any] = {
            "key": self.api_key,
            "city": city,
            "citylimit": "true",
            "offset": offset,
            "page": page,
            "output": "JSON",
        }
        if types:
            params["types"] = types
        if keywords:
            params["keywords"] = keywords

        return await self._request(_TEXT_URL, params, label=f"text/{city}/{types}")

    async def search_around(
        self,
        location: str,
        types: str = "",
        radius: int = 5000,
        page: int = 1,
        offset: int = 25,
    ) -> list[dict[str, Any]]:
        """POI search around a center point.

        Args:
            location: Center point as "lng,lat".
            types: AMAP type codes.
            radius: Search radius in meters (max 50000).
            page: Page number (1-indexed).
            offset: Results per page (max 25).

        Returns:
            Parsed POI dicts.
        """
        if not self.api_key:
            return []

        params: dict[str, Any] = {
            "key": self.api_key,
            "location": location,
            "radius": radius,
            "offset": offset,
            "page": page,
            "output": "JSON",
        }
        if types:
            params["types"] = types

        return await self._request(_AROUND_URL, params, label=f"around/{location}/{types}")

    async def fetch_city_pois(
        self,
        city: str,
        type_codes: dict[str, str],
        pages: int = 3,
        qps_delay: float = 0.6,
    ) -> list[dict[str, Any]]:
        """Fetch all POIs for a city across multiple type categories.

        This is the main entry point for daily_runner.

        Args:
            city: City name.
            type_codes: Mapping of scenario name → AMAP type codes.
            pages: Number of pages to fetch per type (1-3 recommended).
            qps_delay: Seconds to wait between API calls (0.6s for personal
                       key with 2 QPS limit; set to 0 for enterprise key).

        Returns:
            Deduplicated list of POI dicts.
        """
        all_pois: list[dict[str, Any]] = []
        seen_names: set[str] = set()
        request_count = 0

        for scenario, codes in type_codes.items():
            for page in range(1, pages + 1):
                if request_count > 0 and qps_delay > 0 and self.api_key:
                    await asyncio.sleep(qps_delay)
                pois = await self.search_text(city=city, types=codes, page=page)
                request_count += 1
                if not pois:
                    break  # No more results for this type
                for poi in pois:
                    name = poi["name"]
                    if name not in seen_names:
                        seen_names.add(name)
                        poi["scenario"] = scenario
                        all_pois.append(poi)
                logger.debug(
                    "  %s/%s page %d: %d POIs",
                    city, scenario, page, len(pois),
                )

        logger.info("Fetched %d unique POIs for %s", len(all_pois), city)
        return all_pois

    async def close(self) -> None:
        """Close the HTTP client."""
        if self._client:
            await self._client.aclose()
            self._client = None

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _request(
        self, url: str, params: dict[str, Any], label: str = ""
    ) -> list[dict[str, Any]]:
        """Execute an AMAP API request and parse POIs from response."""
        try:
            client = await self._get_client()
            resp = await client.get(url, params=params)
            resp.raise_for_status()
            data = resp.json()

            if data.get("status") != "1":
                logger.warning(
                    "AMAP API error [%s]: status=%s info=%s",
                    label, data.get("status"), data.get("info"),
                )
                return []

            raw_pois = data.get("pois", [])
            return [self._parse_poi(p) for p in raw_pois if p.get("name")]

        except httpx.HTTPStatusError as e:
            logger.error("AMAP HTTP error [%s]: %s", label, e)
            return []
        except Exception as e:
            logger.error("AMAP request failed [%s]: %s", label, e)
            return []

    @staticmethod
    def _parse_poi(raw: dict[str, Any]) -> dict[str, Any]:
        """Convert raw AMAP POI to internal format."""

        def _safe_str(value: Any, default: str = "") -> str:
            """Coerce AMAP field to string. AMAP sometimes returns [] or None
            instead of a string for missing fields."""
            if isinstance(value, str) and value != "[]":
                return value
            return default

        biz_ext = raw.get("biz_ext") or {}
        rating_str = biz_ext.get("rating") if isinstance(biz_ext, dict) else None

        # Parse rating (AMAP returns string like "4.5", "[]", [], or None)
        rating: float | None = None
        if isinstance(rating_str, str) and rating_str not in ("[]", ""):
            try:
                rating = float(rating_str)
            except (ValueError, TypeError):
                pass
        elif isinstance(rating_str, (int, float)):
            rating = float(rating_str)

        # Map AMAP type string to user-friendly tags
        amap_type = _safe_str(raw.get("type"))
        tags = _map_type_to_tags(amap_type)

        return {
            "name": _safe_str(raw.get("name")),
            "address": _safe_str(raw.get("address")),
            "location": _safe_str(raw.get("location")),  # "lng,lat"
            "amap_type": amap_type,
            "tags": tags,
            "rating": rating,
            "phone": _safe_str(raw.get("tel")),
        }

    @staticmethod
    def _mock_search(city: str, types: str) -> list[dict[str, Any]]:
        """Return mock POI data when no API key is configured."""
        return [
            {
                "name": f"{city}示例景点",
                "address": f"{city}市中心路1号",
                "location": "120.635,31.320",
                "amap_type": "风景名胜;公园",
                "tags": ["景点", "户外"],
                "rating": 4.5,
                "phone": "",
            },
        ]


def _map_type_to_tags(amap_type: str) -> list[str]:
    """Map AMAP type string (e.g. '风景名胜;公园') to user-friendly tags."""
    tags: list[str] = []
    seen: set[str] = set()
    # AMAP type is semicolon-separated hierarchy, e.g. "餐饮服务;中餐厅;火锅店"
    parts = [p.strip() for p in amap_type.split(";") if p.strip()]
    for part in parts:
        mapped = AMAP_TYPE_MAPPING.get(part, [])
        for tag in mapped:
            if tag not in seen:
                seen.add(tag)
                tags.append(tag)
    return tags or ["其他"]
