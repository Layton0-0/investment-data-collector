#!/usr/bin/env python3
"""
DART collector 단위 테스트.
- 시그널 키워드(무상증자, 감자 등) 매칭 시 signalRelevant=True, eventType DART_SIGNAL: 접두사 검증.
"""
import unittest
from typing import Any, Dict, List

from collectors.dart_collector import (
    DART_SIGNAL_EVENT_TYPE_PREFIX,
    to_collected_items,
)


def _raw_dart_item(
    report_nm: str = "",
    rcept_no: str = "20240101000001",
    corp_name: str = "",
    flr_nm: str = "",
    rcept_dt: str = "20240101",
    stock_code: str = "",
) -> Dict[str, Any]:
    """DART list API 형식의 raw 항목 생성."""
    return {
        "rcept_no": rcept_no,
        "report_nm": report_nm,
        "corp_name": corp_name,
        "flr_nm": flr_nm,
        "rcept_dt": rcept_dt,
        "stock_code": stock_code,
    }


class TestDartCollectorSignalKeywords(unittest.TestCase):
    """시그널 키워드 매칭 및 eventType 접두사 검증."""

    def test_musangjungja_in_title_sets_signal_relevant_and_event_type_prefix(self) -> None:
        """제목에 '무상증자'가 있으면 signalRelevant=True, eventType이 DART_SIGNAL:으로 시작."""
        raw = _raw_dart_item(report_nm="주요사항보고서(무상증자)", corp_name="테스트회사")
        result: List[Dict[str, Any]] = to_collected_items([raw])
        self.assertEqual(len(result), 1)
        item = result[0]
        self.assertTrue(item["signalRelevant"], "무상증자 키워드 시 signalRelevant=True")
        self.assertTrue(
            item["eventType"].startswith(DART_SIGNAL_EVENT_TYPE_PREFIX),
            f"eventType must start with {DART_SIGNAL_EVENT_TYPE_PREFIX!r}, got {item['eventType'][:50]!r}",
        )
        self.assertEqual(item["source"], "DART")
        self.assertEqual(item["title"], "주요사항보고서(무상증자)")

    def test_gamja_in_title_sets_signal_relevant_and_event_type_prefix(self) -> None:
        """제목에 '감자'가 있으면 signalRelevant=True, eventType이 DART_SIGNAL:으로 시작."""
        raw = _raw_dart_item(report_nm="감자(감소)에 관한 사항", corp_name="A회사")
        result = to_collected_items([raw])
        self.assertEqual(len(result), 1)
        item = result[0]
        self.assertTrue(item["signalRelevant"], "감자 키워드 시 signalRelevant=True")
        self.assertTrue(
            item["eventType"].startswith(DART_SIGNAL_EVENT_TYPE_PREFIX),
            f"eventType must start with {DART_SIGNAL_EVENT_TYPE_PREFIX!r}",
        )

    def test_keyword_in_summary_sets_signal_relevant_and_event_type_prefix(self) -> None:
        """summary(회사명/보고자)에만 키워드가 있어도 signalRelevant=True, eventType DART_SIGNAL:."""
        raw = _raw_dart_item(
            report_nm="기타보고서",
            corp_name="무상증자검증회사",
            flr_nm="대표이사",
        )
        result = to_collected_items([raw])
        self.assertEqual(len(result), 1)
        item = result[0]
        self.assertTrue(item["signalRelevant"], "summary에 무상증자 시 signalRelevant=True")
        self.assertTrue(
            item["eventType"].startswith(DART_SIGNAL_EVENT_TYPE_PREFIX),
            f"eventType must start with {DART_SIGNAL_EVENT_TYPE_PREFIX!r}",
        )

    def test_no_keyword_leaves_signal_relevant_false_and_no_prefix(self) -> None:
        """키워드 없으면 signalRelevant=False, eventType에 DART_SIGNAL: 없음."""
        raw = _raw_dart_item(report_nm="정기보고서", corp_name="일반회사")
        result = to_collected_items([raw])
        self.assertEqual(len(result), 1)
        item = result[0]
        self.assertFalse(item["signalRelevant"])
        self.assertFalse(
            item["eventType"].startswith(DART_SIGNAL_EVENT_TYPE_PREFIX),
            "eventType must not start with DART_SIGNAL: when not signal-relevant",
        )
        self.assertEqual(item["eventType"], "정기보고서")

    def test_payload_shape_unchanged(self) -> None:
        """collected-news 전송용 payload 필드 구성 유지."""
        raw = _raw_dart_item(
            report_nm="무상증자결정",
            rcept_no="20240101000002",
            corp_name="회사명",
            flr_nm="이사",
            rcept_dt="20240301",
            stock_code="005930",
        )
        result = to_collected_items([raw])
        self.assertEqual(len(result), 1)
        item = result[0]
        required_keys = {
            "source", "market", "itemType", "title", "summary", "url",
            "collectedAt", "symbol", "eventType", "signalRelevant",
        }
        self.assertEqual(set(item.keys()), required_keys, "payload shape must match collected-news API")
        self.assertEqual(item["source"], "DART")
        self.assertEqual(item["market"], "KR")
        self.assertEqual(item["itemType"], "FACT")
        self.assertEqual(item["symbol"], "005930")


if __name__ == "__main__":
    unittest.main()
