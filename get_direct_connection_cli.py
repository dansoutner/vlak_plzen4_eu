#!/usr/bin/env python3
"""Generate direction-specific timetables from a GTFS feed."""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import math
import os
import re
import unicodedata
import zipfile
from types import SimpleNamespace
from pathlib import Path
from typing import Any

import pandas as pd
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

DEFAULT_TEMPLATE = """
<!DOCTYPE html>
<html lang="cs">
<head>
  <meta charset="utf-8">
  <title>Jizdni rad</title>
  <style>
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body {
      font-family: Arial, sans-serif;
      padding: 20px;
      background: #fff;
      color: #000;
      min-height: 100vh;
    }
    h1 {
      text-align: center;
      margin-bottom: 24px;
      font-size: 1.8em;
    }
    h2 {
      font-size: 1.1em;
      margin-bottom: 10px;
    }
    .meta-note {
      color: #555;
      font-size: 0.9em;
      margin-top: 8px;
    }
    .stack {
      display: flex;
      flex-direction: column;
      gap: 16px;
    }
    .card {
      border: 1px solid #bbb;
      border-radius: 8px;
      overflow: hidden;
      background: #fff;
    }
    .card-header {
      font-size: 1.05em;
      font-weight: bold;
      padding: 10px 12px;
      background: #efefef;
      border-bottom: 1px solid #bbb;
    }
    .card-content {
      padding: 10px 12px;
    }
    .schedule-container {
      display: grid;
      grid-template-columns: repeat(3, 1fr);
      gap: 16px;
    }
    .schedule-section {
      border: 1px solid #bbb;
      border-radius: 8px;
      overflow: hidden;
    }
    .day-heading {
      font-size: 1.05em;
      font-weight: bold;
      padding: 10px 12px;
      background: #efefef;
      border-bottom: 1px solid #bbb;
    }
    table {
      width: 100%;
      border-collapse: collapse;
    }
    th, td {
      border-bottom: 1px solid #ddd;
      padding: 6px 10px;
      text-align: center;
    }
    th {
      background: #f7f7f7;
    }
    tbody tr:hover {
      background: #f9f9f9;
    }
    .current-hour {
      background: #d9e9ff;
    }
    .minutes-cell {
      display: flex;
      flex-wrap: wrap;
      gap: 6px;
      justify-content: center;
      align-items: center;
    }
    .minute-chip {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      padding: 1px 4px;
      border-radius: 10px;
      border: 1px solid transparent;
      font-variant-numeric: tabular-nums;
    }
    .minute-chip.has-status {
      border-color: #bbb;
      background: #f7f7f7;
    }
    .minute-badge {
      font-size: 0.75em;
      font-weight: bold;
      line-height: 1;
      white-space: nowrap;
    }
    .status-on_time { color: #0b7a3b; }
    .status-delayed { color: #b45309; }
    .status-canceled { color: #b91c1c; }
    .status-diverted { color: #7e22ce; }
    .status-disruption { color: #a21caf; }
    .status-unknown { color: #555; }
    #current-table td, #current-table th {
      padding: 7px 8px;
      text-align: left;
    }
    #current-table td.time-cell {
      width: 62px;
      text-align: right;
      font-variant-numeric: tabular-nums;
    }
    #current-table td.route-cell {
      width: 72px;
      text-align: center;
      font-weight: bold;
    }
    #current-table td.delay-cell {
      width: 92px;
      text-align: center;
      font-weight: bold;
      font-variant-numeric: tabular-nums;
    }
    #current-day-label {
      margin-bottom: 8px;
      color: #333;
      font-size: 0.92em;
    }
    @media (max-width: 980px) {
      .schedule-container {
        grid-template-columns: 1fr;
      }
    }
  </style>
  <script>
    const timetableDepartures = {{ departures_json | safe }};
    const DAY_LABELS = {
      workdays: "Pracovni dny",
      saturday: "Sobota",
      sunday: "Nedele"
    };
    const ENDPOINT_OVERRIDE_KEY = "train_delays_endpoint_override";
    window.DELAYS_ENDPOINT = {{ delays_endpoint_json | safe }};

    let latestDelayRecords = [];
    const debugState = {
      fetch_ok: false,
      fetch_endpoint: null,
      fetch_http_status: null,
      fetch_error: null,
      fetch_attempts: [],
      last_update_iso: null,
      records_count: 0,
      current_selected_count: 0,
      current_match_confidence: { high: 0, medium: 0, unknown: 0 },
      current_status_counts: {},
      current_match_reasons: { train_number: 0, route_code: 0, none: 0 },
      current_train_number_availability: { with_train_number: 0, without_train_number: 0 },
      static_candidate_minutes: 0,
      static_annotated_minutes: 0,
      endpoint_override: null,
      endpoint_candidates: []
    };

    function normalizeText(value) {
      return (value || "")
        .toString()
        .normalize("NFD")
        .replace(/[\\u0300-\\u036f]/g, "")
        .toLowerCase()
        .trim();
    }

    function parseHhmm(value) {
      if (!value) return null;
      const text = String(value).trim();
      const match = text.match(/\\b([0-2]?\\d:[0-5]\\d)\\b/);
      return match ? match[1] : null;
    }

    function hhmmToMinutes(value) {
      const hhmm = parseHhmm(value);
      if (!hhmm) return null;
      const parts = hhmm.split(":");
      return (parseInt(parts[0], 10) * 60) + parseInt(parts[1], 10);
    }

    function parseTrainNumberFromText(value) {
      if (!value) return null;
      const text = String(value);
      const match = text.match(/\\b([a-z]{1,6})\\s*([0-9]{1,6})\\b/i);
      if (!match) return null;
      return Number(match[2]);
    }

    function parseTrainCategoryFromText(value) {
      if (!value) return null;
      const text = String(value);
      const match = text.match(/\\b([a-z]{1,6})\\s*([0-9]{1,6})\\b/i);
      if (!match) return null;
      return match[1];
    }

    function normalizeTrainCategory(value) {
      if (!value) return null;
      const text = String(value);
      const match = text.match(/[a-z]{1,6}/i);
      if (!match) return null;
      return match[0].toLowerCase();
    }

    function currentDayKey(now) {
      const day = now.getDay();
      if (day >= 1 && day <= 5) return "workdays";
      if (day === 6) return "saturday";
      return "sunday";
    }

    function isRouteCodeToken(token) {
      if (!/^[a-z0-9]{2,8}$/.test(token)) return false;
      return /[a-z]/.test(token) && /[0-9]/.test(token);
    }

    function extractRouteCodes(value) {
      const tokens = normalizeText(value).split(/[^a-z0-9]+/).filter(Boolean);
      const codes = tokens.filter((token) => isRouteCodeToken(token));
      return Array.from(new Set(codes));
    }

    function shareRouteCode(leftCodes, rightCodes) {
      if (!leftCodes.length || !rightCodes.length) return false;
      const rightSet = new Set(rightCodes);
      return leftCodes.some((code) => rightSet.has(code));
    }

    function normalizeDelayRecord(record) {
      const status = record && record.status ? String(record.status) : "unknown";
      const delayMinutes = (record && Number.isFinite(Number(record.delay_minutes)))
        ? Number(record.delay_minutes)
        : null;
      const trainNumber = (record && Number.isFinite(Number(record.train_number)))
        ? Number(record.train_number)
        : null;
      const trainCategory = normalizeTrainCategory(
        (record && record.train_category) || parseTrainCategoryFromText(record ? record.train : "")
      );
      const routeCodes = extractRouteCodes(record ? (record.route_text || record.route || "") : "");
      const scheduledMinutes = hhmmToMinutes(record ? (record.scheduled_time_hhmm || record.scheduled_actual_time || "") : "");
      return {
        raw: record,
        status,
        delayMinutes,
        trainNumber,
        trainCategory,
        routeCodes,
        scheduledMinutes,
      };
    }

    function departureMinutes(departure) {
      if (!departure) return null;
      return hhmmToMinutes(departure.departure_time || "");
    }

    function matchDeparture(departure, delayRecords) {
      const depMinutes = departureMinutes(departure);
      if (depMinutes == null) {
        return { status: "unknown", confidence: "none", match_reason: "none", record: null };
      }

      const depTrainNumber = Number.isFinite(Number(departure.train_number))
        ? Number(departure.train_number)
        : parseTrainNumberFromText(departure.route_short_name || "");
      const depTrainCategory = normalizeTrainCategory(
        departure.train_category || parseTrainCategoryFromText(departure.route_short_name || "")
      );

      let strictCandidates = [];
      if (depTrainNumber != null) {
        strictCandidates = delayRecords.filter((record) => {
          if (record.trainNumber == null) return false;
          if (record.trainNumber !== depTrainNumber) return false;
          if (depTrainCategory && record.trainCategory && record.trainCategory !== depTrainCategory) return false;
          return true;
        });
      }

      if (strictCandidates.length === 1) {
        return { status: strictCandidates[0].status, confidence: "high", match_reason: "train_number", record: strictCandidates[0] };
      }
      if (strictCandidates.length > 1) {
        const timeFilteredStrictCandidates = strictCandidates.filter((record) => {
          if (record.scheduledMinutes == null) return false;
          return Math.abs(record.scheduledMinutes - depMinutes) <= 3;
        });
        if (timeFilteredStrictCandidates.length === 1) {
          return {
            status: timeFilteredStrictCandidates[0].status,
            confidence: "high",
            match_reason: "train_number",
            record: timeFilteredStrictCandidates[0],
          };
        }
        return { status: "unknown", confidence: "none", match_reason: "none", record: null };
      }

      const depRouteCodes = extractRouteCodes(departure.route_short_name || "");
      if (!depRouteCodes.length) {
        return { status: "unknown", confidence: "none", match_reason: "none", record: null };
      }

      const routeCodeCandidates = delayRecords.filter((record) => {
        if (record.scheduledMinutes == null) return false;
        if (Math.abs(record.scheduledMinutes - depMinutes) > 3) return false;
        if (!record.routeCodes || !record.routeCodes.length) return false;
        return shareRouteCode(depRouteCodes, record.routeCodes);
      });

      if (routeCodeCandidates.length === 1) {
        return { status: routeCodeCandidates[0].status, confidence: "medium", match_reason: "route_code", record: routeCodeCandidates[0] };
      }
      if (routeCodeCandidates.length > 1) {
        return { status: "unknown", confidence: "none", match_reason: "none", record: null };
      }

      return { status: "unknown", confidence: "none", match_reason: "none", record: null };
    }

    function statusClass(status) {
      return `status-${status || "unknown"}`;
    }

    function statusLabel(match) {
      const status = match.status || "unknown";
      const delayMinutes = match.record ? match.record.delayMinutes : null;
      if (status === "on_time") return "vcas";
      if (status === "delayed") {
        return delayMinutes == null ? "zpozdeni" : `+${delayMinutes}`;
      }
      if (status === "canceled") return "zrusen";
      if (status === "diverted") return "odklon";
      if (status === "disruption") return "vyluka";
      return "nezname";
    }

    function countsToText(counts) {
      const keys = Object.keys(counts || {}).sort();
      if (!keys.length) return "none";
      return keys.map((key) => `${key}:${counts[key]}`).join(", ");
    }

    function getUrlEndpointOverride() {
      try {
        const params = new URLSearchParams(window.location.search || "");
        const value = (params.get("delays_endpoint") || "").trim();
        return value || null;
      } catch (_error) {
        return null;
      }
    }

    function getStoredEndpointOverride() {
      try {
        const value = (window.localStorage.getItem(ENDPOINT_OVERRIDE_KEY) || "").trim();
        return value || null;
      } catch (_error) {
        return null;
      }
    }

    function setStoredEndpointOverride(value) {
      try {
        const text = (value || "").trim();
        if (!text) {
          window.localStorage.removeItem(ENDPOINT_OVERRIDE_KEY);
        } else {
          window.localStorage.setItem(ENDPOINT_OVERRIDE_KEY, text);
        }
      } catch (_error) {
        // ignore localStorage errors
      }
    }

    function getActiveEndpointOverride() {
      return getUrlEndpointOverride() || getStoredEndpointOverride();
    }

    function highlightCurrentHour(dayKey, currentHour) {
      const rows = document.querySelectorAll(`.${dayKey} tbody tr`);
      rows.forEach((row) => {
        row.classList.remove("current-hour");
        const hourCell = row.querySelector("td:first-child");
        if (hourCell && parseInt(hourCell.textContent, 10) === currentHour) {
          row.classList.add("current-hour");
        }
      });
    }

    function renderCurrentDepartures(dayKey) {
      const tableBody = document.getElementById("current-departures-body");
      const label = document.getElementById("current-day-label");
      if (!tableBody || !label) {
        return {
          selectedCount: 0,
          confidenceCounts: { high: 0, medium: 0, unknown: 0 },
          statusCounts: {},
          reasonCounts: { train_number: 0, route_code: 0, none: 0 },
          trainNumberAvailability: { with_train_number: 0, without_train_number: 0 },
        };
      }

      label.textContent = `Aktivni den: ${DAY_LABELS[dayKey] || dayKey}`;

      const allDepartures = (timetableDepartures && timetableDepartures[dayKey]) || [];
      const now = new Date();
      const nowMinutes = now.getHours() * 60 + now.getMinutes();
      const futureDepartures = allDepartures
        .slice()
        .sort((a, b) => departureMinutes(a) - departureMinutes(b))
        .filter((departure) => {
          const depMinutes = departureMinutes(departure);
          return depMinutes != null && depMinutes >= (nowMinutes - 5);
        });

      const selected = (futureDepartures.length ? futureDepartures : allDepartures.slice(0))
        .slice(0, 10);

      if (!selected.length) {
        tableBody.innerHTML = '<tr><td colspan="4">Pro tento den nejsou dostupne odjezdy.</td></tr>';
        return {
          selectedCount: 0,
          confidenceCounts: { high: 0, medium: 0, unknown: 0 },
          statusCounts: {},
          reasonCounts: { train_number: 0, route_code: 0, none: 0 },
          trainNumberAvailability: { with_train_number: 0, without_train_number: 0 },
        };
      }

      const confidenceCounts = { high: 0, medium: 0, unknown: 0 };
      const statusCounts = {};
      const reasonCounts = { train_number: 0, route_code: 0, none: 0 };
      const trainNumberAvailability = { with_train_number: 0, without_train_number: 0 };

      const rows = selected.map((departure) => {
        const match = matchDeparture(departure, latestDelayRecords);
        if (match.confidence === "high") confidenceCounts.high += 1;
        else if (match.confidence === "medium") confidenceCounts.medium += 1;
        else confidenceCounts.unknown += 1;

        const matchStatus = match.status || "unknown";
        statusCounts[matchStatus] = (statusCounts[matchStatus] || 0) + 1;
        const reason = match.match_reason || "none";
        reasonCounts[reason] = (reasonCounts[reason] || 0) + 1;
        const departureTrainNumber = Number.isFinite(Number(departure.train_number))
          ? Number(departure.train_number)
          : parseTrainNumberFromText(departure.route_short_name || "");
        if (departureTrainNumber == null) trainNumberAvailability.without_train_number += 1;
        else trainNumberAvailability.with_train_number += 1;

        const cls = statusClass(match.status);
        let confidenceText = "nezname";
        if (match.confidence === "high") confidenceText = "vysoka shoda";
        else if (match.confidence === "medium") confidenceText = "stredni shoda";
        const statusText = statusLabel(match);
        const timeLabel = parseHhmm(departure.departure_time) || departure.departure_time || "--:--";
        return `
          <tr>
            <td class="time-cell">${timeLabel}</td>
            <td class="route-cell">${departure.route_short_name || "-"}</td>
            <td>${departure.route_long_name || "-"}</td>
            <td class="delay-cell ${cls}" title="${confidenceText}">${statusText}</td>
          </tr>
        `;
      });

      tableBody.innerHTML = rows.join("");
      return {
        selectedCount: selected.length,
        confidenceCounts,
        statusCounts,
        reasonCounts,
        trainNumberAvailability,
      };
    }

    function clearMinuteBadges() {
      document.querySelectorAll(".minute-chip").forEach((chip) => {
        chip.classList.remove("has-status");
        const badge = chip.querySelector(".minute-badge");
        if (badge) {
          badge.hidden = true;
          badge.textContent = "";
          badge.className = "minute-badge";
        }
      });
    }

    function annotateStaticMinutes(activeDayKey) {
      clearMinuteBadges();

      const minuteMatches = {};
      const departuresForActiveDay = (timetableDepartures && timetableDepartures[activeDayKey]) || [];
      departuresForActiveDay.forEach((departure) => {
        const match = matchDeparture(departure, latestDelayRecords);
        if (match.confidence !== "high") return;
        const key = `${activeDayKey}|${departure.hour}|${departure.minute}`;
        if (!minuteMatches[key]) minuteMatches[key] = [];
        minuteMatches[key].push(match);
      });

      let annotatedMinutes = 0;
      Object.entries(minuteMatches).forEach(([key, matches]) => {
        const statuses = Array.from(new Set(matches.map((match) => match.status || "unknown")));
        if (statuses.length !== 1) return;
        const status = statuses[0];
        if (status === "unknown") return;

        let label = statusLabel(matches[0]);
        if (status === "delayed") {
          const values = Array.from(new Set(matches.map((match) => match.record ? match.record.delayMinutes : null)));
          if (values.length !== 1 || values[0] == null) {
            label = "zpozdeni";
          }
        }

        const parts = key.split("|");
        const dayKey = parts[0];
        const hour = parts[1];
        const minute = parts[2];
        const chips = document.querySelectorAll(`.minute-chip[data-day="${dayKey}"][data-hour="${hour}"][data-minute="${minute}"]`);
        chips.forEach((chip) => {
          const badge = chip.querySelector(".minute-badge");
          if (!badge) return;
          chip.classList.add("has-status");
          badge.hidden = false;
          badge.textContent = label;
          badge.classList.add(statusClass(status));
          annotatedMinutes += 1;
        });
      });

      return {
        candidateMinutes: Object.keys(minuteMatches).length,
        annotatedMinutes,
      };
    }

    function delayEndpointCandidates() {
      const candidates = [];
      const endpointOverride = getActiveEndpointOverride();
      if (endpointOverride) {
        candidates.push(endpointOverride);
      }
      if (window.DELAYS_ENDPOINT) {
        candidates.push(String(window.DELAYS_ENDPOINT));
      }
      candidates.push("/train_delays");
      if (window.location && window.location.origin && window.location.origin !== "null") {
        candidates.push(`${window.location.origin}/train_delays`);
      }
      if (window.location && window.location.protocol === "file:") {
        candidates.push("http://127.0.0.1:5000/train_delays");
        candidates.push("http://localhost:5000/train_delays");
      }
      debugState.endpoint_override = endpointOverride;
      debugState.endpoint_candidates = Array.from(new Set(candidates));
      return debugState.endpoint_candidates;
    }

    async function refreshDelays() {
      const endpoints = delayEndpointCandidates();
      debugState.fetch_attempts = [];
      for (const endpoint of endpoints) {
        try {
          const response = await fetch(endpoint, { cache: "no-store" });
          debugState.fetch_attempts.push(`${endpoint} -> HTTP ${response.status}`);
          if (!response.ok) {
            throw new Error(`HTTP ${response.status}`);
          }
          const payload = await response.json();
          latestDelayRecords = Object.values(payload || {}).map((record) => normalizeDelayRecord(record));

          debugState.fetch_ok = true;
          debugState.fetch_endpoint = endpoint;
          debugState.fetch_http_status = response.status;
          debugState.fetch_error = null;
          debugState.records_count = latestDelayRecords.length;
          debugState.last_update_iso = new Date().toISOString();
          return;
        } catch (error) {
          debugState.fetch_ok = false;
          debugState.fetch_endpoint = endpoint;
          debugState.fetch_http_status = null;
          debugState.fetch_error = String(error && error.message ? error.message : error);
        }
      }
      debugState.last_update_iso = new Date().toISOString();
      console.warn("Failed to refresh train delays:", debugState.fetch_error);
    }

    async function refreshView() {
      const now = new Date();
      const dayKey = currentDayKey(now);
      highlightCurrentHour(dayKey, now.getHours());
      await refreshDelays();
      const currentStats = renderCurrentDepartures(dayKey);
      const staticStats = annotateStaticMinutes(dayKey);
      debugState.current_selected_count = currentStats.selectedCount;
      debugState.current_match_confidence = currentStats.confidenceCounts;
      debugState.current_status_counts = currentStats.statusCounts;
      debugState.current_match_reasons = currentStats.reasonCounts;
      debugState.current_train_number_availability = currentStats.trainNumberAvailability;
      debugState.static_candidate_minutes = staticStats.candidateMinutes;
      debugState.static_annotated_minutes = staticStats.annotatedMinutes;
    }

    document.addEventListener("DOMContentLoaded", () => {
      refreshView();
      window.setInterval(refreshView, 60 * 1000);
    });
  </script>
</head>
<body>
  <div class="stack">
    <h1>{{ title }}</h1>

  <div class="schedule-container">
    <div class="schedule-section workdays">
      <div class="day-heading">PRACOVNÍ DNY</div>
      <table>
        <thead><tr><th>Hodina</th><th>Minuty</th></tr></thead>
        <tbody>
        {% for hour, minutes in workdays.items() | sort %}
          <tr><td>{{ hour }}</td><td class="minutes-cell">{% for minute in minutes %}<span class="minute-chip" data-day="workdays" data-hour="{{ hour }}" data-minute="{{ minute }}">{{ minute }}<span class="minute-badge" hidden></span></span>{% endfor %}</td></tr>
        {% endfor %}
        </tbody>
      </table>
    </div>

    <div class="schedule-section saturday">
      <div class="day-heading">SOBOTA</div>
      <table>
        <thead><tr><th>Hodina</th><th>Minuty</th></tr></thead>
        <tbody>
        {% for hour, minutes in saturday.items() | sort %}
          <tr><td>{{ hour }}</td><td class="minutes-cell">{% for minute in minutes %}<span class="minute-chip" data-day="saturday" data-hour="{{ hour }}" data-minute="{{ minute }}">{{ minute }}<span class="minute-badge" hidden></span></span>{% endfor %}</td></tr>
        {% endfor %}
        </tbody>
      </table>
    </div>

    <div class="schedule-section sunday">
      <div class="day-heading">NEDĚLE</div>
      <table>
        <thead><tr><th>Hodina</th><th>Minuty</th></tr></thead>
        <tbody>
        {% for hour, minutes in sunday.items() | sort %}
          <tr><td>{{ hour }}</td><td class="minutes-cell">{% for minute in minutes %}<span class="minute-chip" data-day="sunday" data-hour="{{ hour }}" data-minute="{{ minute }}">{{ minute }}<span class="minute-badge" hidden></span></span>{% endfor %}</td></tr>
        {% endfor %}
        </tbody>
      </table>
    </div>
  </div>

    <section class="card">
      <div class="card-header">Aktualni odjezdy</div>
      <div class="card-content">
        <div id="current-day-label"></div>
        <table id="current-table">
          <thead>
            <tr><th>Cas</th><th>Linka</th><th>Smer</th><th>Zpozdeni</th></tr>
          </thead>
          <tbody id="current-departures-body">
            <tr><td colspan="4">Nacitam data...</td></tr>
          </tbody>
        </table>
        <p class="meta-note">Stavy se aktualizuji kazdych 60 sekund. Chybejici zaznam znamena neznamy stav, ne vcasny vlak.</p>
      </div>
    </section>
  </div>
</body>
</html>
""".strip()

DEFAULT_GTFS_CANDIDATES = [
    Path("jizdni-rady-czech-republic/data/merged")
]


def seconds_to_time(value: Any) -> str | None:
    """Convert seconds since midnight to HH:MM:SS."""
    if value is None:
        return None
    try:
        if math.isnan(value):
            return None
    except (TypeError, ValueError):
        pass
    if value == "":
        return None
    total = int(value)
    hours = total // 3600
    minutes = (total % 3600) // 60
    seconds = total % 60
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"


def group_by_hour(departure_times: list[str | None]) -> dict[int, list[str]]:
    """Group departure times by hour and list minutes in each hour."""
    hour_map: dict[int, set[str]] = {}
    for departure in departure_times:
        if not departure:
            continue
        hh, mm, _ = departure.split(":")
        hour = int(hh)
        hour_map.setdefault(hour, set()).add(mm)

    grouped: dict[int, list[str]] = {}
    for hour, minutes in sorted(hour_map.items()):
        grouped[hour] = sorted(minutes)
    return grouped


def normalize_for_matching(value: Any) -> str:
    text = str(value or "")
    normalized = unicodedata.normalize("NFKD", text)
    without_diacritics = "".join(char for char in normalized if not unicodedata.combining(char))
    return without_diacritics.lower().strip()


def extract_hhmm(value: Any) -> str | None:
    if value is None:
        return None
    match = re.search(r"\b([0-2]?\d:[0-5]\d)\b", str(value))
    if not match:
        return None
    return match.group(1)


def hhmm_to_minutes(value: Any) -> int | None:
    hhmm = extract_hhmm(value)
    if not hhmm:
        return None
    hh, mm = hhmm.split(":")
    return int(hh) * 60 + int(mm)


def is_route_code_token(token: str) -> bool:
    if not re.fullmatch(r"[a-z0-9]{2,8}", token):
        return False
    has_alpha = any(char.isalpha() for char in token)
    has_digit = any(char.isdigit() for char in token)
    return has_alpha and has_digit


def extract_route_codes(value: Any) -> set[str]:
    normalized = normalize_for_matching(value)
    tokens = {token for token in re.split(r"[^a-z0-9]+", normalized) if token}
    return {token for token in tokens if is_route_code_token(token)}


def match_departure_to_delay_records(departure: dict[str, Any], delay_records: list[dict[str, Any]]) -> dict[str, Any]:
    dep_minutes = hhmm_to_minutes(departure.get("departure_time"))
    if dep_minutes is None:
        return {"status": "unknown", "confidence": "none", "match_reason": "none", "record": None}

    dep_train_category = departure.get("train_category")
    dep_train_number = departure.get("train_number")
    parsed_category, parsed_number = parse_train_identity(departure.get("route_short_name"))
    if dep_train_category is None:
        dep_train_category = parsed_category
    if dep_train_number is None:
        dep_train_number = parsed_number

    dep_train_category_norm = normalize_for_matching(dep_train_category) if dep_train_category else None
    strict_candidates: list[dict[str, Any]] = []
    if dep_train_number is not None:
        for record in delay_records:
            record_train_number = record.get("train_number")
            if record_train_number is None:
                continue
            if int(record_train_number) != int(dep_train_number):
                continue
            record_train_category = record.get("train_category")
            if dep_train_category_norm and record_train_category:
                if normalize_for_matching(record_train_category) != dep_train_category_norm:
                    continue
            strict_candidates.append(record)

    if len(strict_candidates) == 1:
        return {
            "status": strict_candidates[0].get("status", "unknown"),
            "confidence": "high",
            "match_reason": "train_number",
            "record": strict_candidates[0],
        }
    if len(strict_candidates) > 1:
        time_filtered_candidates: list[dict[str, Any]] = []
        for record in strict_candidates:
            record_minutes = hhmm_to_minutes(record.get("scheduled_time_hhmm") or record.get("scheduled_actual_time"))
            if record_minutes is None:
                continue
            if abs(record_minutes - dep_minutes) <= 3:
                time_filtered_candidates.append(record)
        if len(time_filtered_candidates) == 1:
            return {
                "status": time_filtered_candidates[0].get("status", "unknown"),
                "confidence": "high",
                "match_reason": "train_number",
                "record": time_filtered_candidates[0],
            }
        return {"status": "unknown", "confidence": "none", "match_reason": "none", "record": None}

    dep_route_codes = extract_route_codes(departure.get("route_short_name"))
    if not dep_route_codes:
        return {"status": "unknown", "confidence": "none", "match_reason": "none", "record": None}

    route_code_candidates: list[dict[str, Any]] = []
    for record in delay_records:
        record_minutes = hhmm_to_minutes(record.get("scheduled_time_hhmm") or record.get("scheduled_actual_time"))
        if record_minutes is None or abs(record_minutes - dep_minutes) > 3:
            continue
        route_text = record.get("route_text") or record.get("route")
        record_route_codes = extract_route_codes(route_text)
        if not (dep_route_codes & record_route_codes):
            continue
        route_code_candidates.append(record)

    if len(route_code_candidates) == 1:
        return {
            "status": route_code_candidates[0].get("status", "unknown"),
            "confidence": "medium",
            "match_reason": "route_code",
            "record": route_code_candidates[0],
        }
    if len(route_code_candidates) > 1:
        return {"status": "unknown", "confidence": "none", "match_reason": "none", "record": None}

    return {"status": "unknown", "confidence": "none", "match_reason": "none", "record": None}


def parse_train_identity(value: Any) -> tuple[str | None, int | None]:
    if value is None:
        return None, None
    match = re.search(r"\b([A-Za-z]{1,6})\s*([0-9]{1,6})\b", str(value))
    if not match:
        return None, None
    return match.group(1), int(match.group(2))


def safe_text(value: Any) -> str:
    if value is None:
        return ""
    try:
        if pd.isna(value):
            return ""
    except TypeError:
        pass
    return str(value)


def build_departure_records(rows: pd.DataFrame, station_id_from: str, station_id_to: str) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    if rows.empty:
        return records

    sorted_rows = rows.sort_values(["departure_time", "trip_id"], kind="stable")
    for row in sorted_rows.to_dict(orient="records"):
        departure_time = row.get("departure_time")
        if not departure_time:
            continue
        hh, mm, _ = str(departure_time).split(":")
        train_category, train_number = parse_train_identity(row.get("trip_short_name"))
        if train_number is None:
            fallback_category, fallback_number = parse_train_identity(row.get("route_short_name"))
            if fallback_number is not None:
                train_category = fallback_category
                train_number = fallback_number
        records.append({
            "trip_id": safe_text(row.get("trip_id")),
            "route_id": safe_text(row.get("route_id")),
            "route_short_name": safe_text(row.get("route_short_name")),
            "route_long_name": safe_text(row.get("route_long_name")),
            "departure_time": departure_time,
            "hour": int(hh),
            "minute": mm,
            "from_stop_id": station_id_from,
            "to_stop_id": station_id_to,
            "train_category": train_category,
            "train_number": train_number,
        })
    return records


def build_timetable(feed: Any, station_id_from: str, station_id_to: str) -> dict[str, Any]:
    """Extract timetable rows where next stop is station_id_to."""
    logger.debug(f"Building timetable from stop {station_id_from} to {station_id_to}")

    stop_times = feed.stop_times[["trip_id", "stop_id", "stop_sequence", "departure_time"]].copy()
    logger.debug(f"Total stop_times rows: {len(stop_times)}")

    stop_times.sort_values(["trip_id", "stop_sequence"], inplace=True)
    stop_times["next_stop_id"] = stop_times.groupby("trip_id")["stop_id"].shift(-1)

    matches = stop_times[
        (stop_times["stop_id"].astype(str) == str(station_id_from))
        & (stop_times["next_stop_id"].astype(str) == str(station_id_to))
    ].copy()
    logger.debug(f"Found {len(matches)} matching stop sequences")

    matches["departure_time"] = matches["departure_time"].apply(seconds_to_time)

    trip_columns = ["trip_id", "service_id", "route_id"]
    for optional_column in ["trip_short_name", "trip_headsign"]:
        if optional_column in feed.trips.columns:
            trip_columns.append(optional_column)

    trips = feed.trips[trip_columns]
    logger.debug(f"Total trips: {len(trips)}")

    station_trips = matches.merge(trips, on="trip_id", how="left")

    route_columns = ["route_id"]
    for optional_column in ["route_short_name", "route_long_name"]:
        if optional_column in feed.routes.columns:
            route_columns.append(optional_column)
    route_table = feed.routes[route_columns].drop_duplicates()
    station_trips = station_trips.merge(route_table, on="route_id", how="left")

    logger.debug(f"After merging with trips: {len(station_trips)} rows")

    if station_trips.empty:
        logger.debug("No trips found for the requested adjacent stop pair")
        return {
            "workdays": {},
            "saturday": {},
            "sunday": {},
            "departures": {"workdays": [], "saturday": [], "sunday": []},
        }

    calendar_service_days = feed.calendar[["service_id", "day_of_week"]].drop_duplicates()
    logger.debug(f"Unique service_id + day_of_week combinations: {len(calendar_service_days)}")

    service_days_by_service = (
        calendar_service_days.groupby("service_id")["day_of_week"]
        .agg(lambda values: frozenset(int(day) for day in values.dropna()))
        .to_dict()
    )
    station_trips["service_days"] = station_trips["service_id"].map(service_days_by_service)
    logger.debug(f"Unique service_ids in matches: {station_trips['service_id'].dropna().unique()[:10]}")
    logger.debug(
        "Station trips with known service days: %s/%s",
        station_trips["service_days"].notna().sum(),
        len(station_trips),
    )

    def runs_on(days: Any, target_days: set[int]) -> bool:
        return isinstance(days, frozenset) and bool(days & target_days)

    workday_rows = station_trips[
        station_trips["service_days"].apply(lambda days: runs_on(days, {0, 1, 2, 3, 4}))
    ].copy()
    saturday_rows = station_trips[
        station_trips["service_days"].apply(lambda days: runs_on(days, {5}))
    ].copy()
    sunday_rows = station_trips[
        station_trips["service_days"].apply(lambda days: runs_on(days, {6}))
    ].copy()

    workday_departures = workday_rows["departure_time"].tolist()
    saturday_departures = saturday_rows["departure_time"].tolist()
    sunday_departures = sunday_rows["departure_time"].tolist()

    departures = {
        "workdays": build_departure_records(workday_rows, station_id_from, station_id_to),
        "saturday": build_departure_records(saturday_rows, station_id_from, station_id_to),
        "sunday": build_departure_records(sunday_rows, station_id_from, station_id_to),
    }

    logger.debug(f"Workday departures: {len(workday_departures)}")
    logger.debug(f"Saturday departures: {len(saturday_departures)}")
    logger.debug(f"Sunday departures: {len(sunday_departures)}")
    logger.debug(
        "Detailed departures: workdays=%s saturday=%s sunday=%s",
        len(departures["workdays"]),
        len(departures["saturday"]),
        len(departures["sunday"]),
    )

    return {
        "workdays": group_by_hour(workday_departures),
        "saturday": group_by_hour(saturday_departures),
        "sunday": group_by_hour(sunday_departures),
        "departures": departures,
    }


def render_html(
    timetable: dict[str, Any],
    template_str: str,
    title: str,
    delays_endpoint: str | None = None,
) -> str:
    """Render timetable to HTML."""
    try:
        from jinja2 import Template
    except ImportError as exc:
        raise SystemExit("Missing dependency: jinja2. Install it with `pip install jinja2`.") from exc
    departures = timetable.get("departures", {"workdays": [], "saturday": [], "sunday": []})
    return Template(template_str).render(
        title=title,
        workdays=timetable["workdays"],
        saturday=timetable["saturday"],
        sunday=timetable["sunday"],
        departures=departures,
        departures_json=json.dumps(departures, ensure_ascii=False),
        delays_endpoint_json=json.dumps(delays_endpoint, ensure_ascii=False),
    )


def slugify(value: str) -> str:
    result = re.sub(r"[^a-zA-Z0-9]+", "_", value.strip().lower()).strip("_")
    return result or "timetable"


def write_text(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def resolve_gtfs_path(explicit_path: str | None) -> Path:
    if explicit_path:
        path = Path(explicit_path)
        if not path.exists():
            raise FileNotFoundError(f"GTFS feed not found: {path}")
        return path
    for candidate in DEFAULT_GTFS_CANDIDATES:
        if candidate.exists():
            return candidate
    searched = ", ".join(str(p) for p in DEFAULT_GTFS_CANDIDATES)
    raise FileNotFoundError(f"No GTFS feed found. Tried: {searched}. Use --gtfs-path.")


def default_output_name(from_label: str, to_label: str) -> str:
    return f"{slugify(from_label)}_{slugify(to_label)}.html"


def load_gtfs_feed(gtfs_path: Path) -> Any:
    """Load only GTFS tables used by this CLI without partridge/networkx."""
    logger.debug(f"Loading GTFS feed from: {gtfs_path}")

    required_files = ["stop_times.txt", "trips.txt", "routes.txt"]
    optional_files = ["calendar.txt", "calendar_dates.txt"]
    tables: dict[str, Any] = {}

    if gtfs_path.is_dir():
        logger.debug("GTFS path is a directory")
        for filename in [*required_files, *optional_files]:
            required = filename in required_files
            file_path = gtfs_path / filename
            file_path_gz = file_path.parent / f"{file_path.name}.gz"
            logger.debug(f"Loading GTFS file: {filename}")
            if file_path_gz.exists():
                logger.debug(f"  Found gzipped version: {file_path_gz}")
                tables[filename] = pd.read_csv(file_path_gz, dtype=str, compression="gzip")
                logger.debug(f"  Loaded {len(tables[filename])} rows")
                continue
            if not file_path.exists():
                if required:
                    raise FileNotFoundError(f"Missing GTFS file: {file_path}")
                logger.debug(f"  Optional file missing: {file_path}")
                tables[filename] = None
                continue
            logger.debug(f"  Loading from: {file_path}")
            tables[filename] = pd.read_csv(file_path, dtype=str)
            logger.debug(f"  Loaded {len(tables[filename])} rows")
    else:
        logger.debug("GTFS path is a zip file")
        with zipfile.ZipFile(gtfs_path) as archive:
            members = archive.namelist()
            available = set(members)
            logger.debug(f"Available files in zip: {sorted(available)}")

            def find_member(filename: str) -> str | None:
                candidates = (filename, f"{filename}.gz")
                for candidate in candidates:
                    if candidate in available:
                        return candidate
                for member in members:
                    for candidate in candidates:
                        if member.endswith(f"/{candidate}"):
                            return member
                return None

            for filename in [*required_files, *optional_files]:
                required = filename in required_files
                member = find_member(filename)
                if member is None:
                    if required:
                        raise FileNotFoundError(f"Missing GTFS file in zip: {filename}")
                    logger.debug(f"  Optional file missing in zip: {filename}")
                    tables[filename] = None
                    continue

                with archive.open(member) as file_obj:
                    if member.endswith(".gz"):
                        with gzip.open(file_obj, mode="rt", encoding="utf-8") as unzipped:
                            tables[filename] = pd.read_csv(unzipped, dtype=str)
                    else:
                        tables[filename] = pd.read_csv(file_obj, dtype=str)
                    logger.debug(f"  Loaded {filename} from {member}: {len(tables[filename])} rows")

    stop_times = tables["stop_times.txt"].copy()
    stop_times["stop_sequence"] = stop_times["stop_sequence"].astype(int)
    stop_times["departure_time"] = stop_times["departure_time"].apply(_parse_gtfs_time_to_seconds)
    logger.debug(f"Processed stop_times: {len(stop_times)} rows")

    service_day_frames: list[pd.DataFrame] = []
    has_calendar = tables.get("calendar.txt") is not None

    calendar_table = tables.get("calendar.txt")
    if calendar_table is not None:
        calendar = calendar_table.copy()
        calendar["service_id"] = calendar["service_id"].astype(str)
        weekday_columns = [
            ("monday", 0),
            ("tuesday", 1),
            ("wednesday", 2),
            ("thursday", 3),
            ("friday", 4),
            ("saturday", 5),
            ("sunday", 6),
        ]

        missing_columns = [name for name, _ in weekday_columns if name not in calendar.columns]
        if missing_columns:
            logger.warning(f"calendar.txt is missing weekday columns: {missing_columns}")

        calendar_rows = 0
        for column_name, day_of_week in weekday_columns:
            if column_name not in calendar.columns:
                continue
            active_services = calendar.loc[
                calendar[column_name].astype(str) == "1", ["service_id"]
            ].copy()
            if active_services.empty:
                continue
            active_services["day_of_week"] = day_of_week
            calendar_rows += len(active_services)
            service_day_frames.append(active_services)
        logger.debug(f"Derived {calendar_rows} service/day rows from calendar.txt")

    calendar_dates_table = tables.get("calendar_dates.txt")
    if calendar_dates_table is not None:
        calendar_dates = calendar_dates_table.copy()
        logger.debug(f"Calendar dates before processing: {len(calendar_dates)} rows")

        if "service_id" not in calendar_dates.columns or "date" not in calendar_dates.columns:
            raise ValueError("calendar_dates.txt must contain service_id and date columns")

        calendar_dates["date"] = pd.to_datetime(calendar_dates["date"], format="%Y%m%d", errors="coerce")
        calendar_dates = calendar_dates.dropna(subset=["date"]).copy()
        calendar_dates["day_of_week"] = calendar_dates["date"].dt.dayofweek.astype(int)
        calendar_dates["service_id"] = calendar_dates["service_id"].astype(str)

        if has_calendar:
            if "exception_type" in calendar_dates.columns:
                active_dates = calendar_dates[calendar_dates["exception_type"] == "1"].copy()
                logger.debug(
                    "calendar.txt present; ignoring %s calendar_dates type=1 rows for regular day-of-week buckets",
                    len(active_dates),
                )
            else:
                logger.debug(
                    "calendar.txt present and calendar_dates has no exception_type; ignoring %s rows for regular day-of-week buckets",
                    len(calendar_dates),
                )
        else:
            # Fallback for feeds that provide only calendar_dates.txt (no calendar.txt).
            # Infer regular service days from exception patterns.
            # - services with REMOVE rows (type=2): treat those weekdays as regular
            # - services with only ADD rows (type=1): treat observed weekdays as regular
            observed_days: pd.DataFrame
            if "exception_type" in calendar_dates.columns:
                service_day_types = (
                    calendar_dates.groupby(["service_id", "day_of_week"])["exception_type"]
                    .agg(lambda values: set(str(v) for v in values.dropna()))
                    .to_dict()
                )
                service_has_remove = (
                    calendar_dates.groupby("service_id")["exception_type"]
                    .agg(lambda values: bool((values == "2").any()))
                    .to_dict()
                )
                service_exception_counts = (
                    calendar_dates.groupby("service_id")["exception_type"]
                    .agg(lambda values: values.value_counts().to_dict())
                    .to_dict()
                )
                service_row_counts = calendar_dates.groupby("service_id").size().to_dict()

                inferred_rows: list[tuple[str, int]] = []
                weekend_fill_count = 0
                sparse_remove_all_days_count = 0
                sparse_remove_row_threshold = 7
                for service_id, has_remove in service_has_remove.items():
                    exception_counts = service_exception_counts.get(service_id, {})
                    has_add = exception_counts.get("1", 0) > 0
                    remove_only = has_remove and not has_add
                    row_count = int(service_row_counts.get(service_id, 0))

                    days: set[int] = set()
                    if remove_only and row_count <= sparse_remove_row_threshold:
                        # Very sparse remove-only exceptions most likely indicate
                        # occasional blackout dates for an otherwise regular service.
                        days = set(range(7))
                        sparse_remove_all_days_count += 1
                    else:
                        for day_of_week in range(7):
                            day_types = service_day_types.get((service_id, day_of_week), set())
                            if has_remove:
                                if "2" in day_types:
                                    days.add(day_of_week)
                            else:
                                if "1" in day_types:
                                    days.add(day_of_week)

                        if has_remove:
                            # Keep weekend balanced when one weekend day is missing.
                            if 6 in days and 5 not in days:
                                days.add(5)
                                weekend_fill_count += 1
                            if 5 in days and 6 not in days:
                                days.add(6)
                                weekend_fill_count += 1

                    for day_of_week in sorted(days):
                        inferred_rows.append((service_id, day_of_week))

                observed_days = pd.DataFrame(
                    inferred_rows,
                    columns=["service_id", "day_of_week"],
                ).drop_duplicates()
                logger.debug(
                    "No calendar.txt; inferred service/day rows from calendar_dates exception patterns: %s",
                    len(observed_days),
                )
                if weekend_fill_count:
                    logger.debug(
                        "No calendar.txt; inferred complementary weekend days for %s service/day pairs",
                        weekend_fill_count,
                    )
                if sparse_remove_all_days_count:
                    logger.debug(
                        "No calendar.txt; inferred all weekdays for %s sparse remove-only services (<=%s rows)",
                        sparse_remove_all_days_count,
                        sparse_remove_row_threshold,
                    )
            else:
                observed_days = calendar_dates[["service_id", "day_of_week"]].drop_duplicates().copy()

            service_day_frames.append(observed_days)
            logger.debug(
                "No calendar.txt available; using all observed service/day rows from calendar_dates.txt: %s",
                len(observed_days),
            )

    if not service_day_frames:
        raise FileNotFoundError("GTFS feed must contain calendar.txt and/or calendar_dates.txt")

    calendar_days = pd.concat(service_day_frames, ignore_index=True)
    calendar_days = calendar_days.dropna(subset=["service_id", "day_of_week"]).copy()
    calendar_days["service_id"] = calendar_days["service_id"].astype(str)
    calendar_days["day_of_week"] = calendar_days["day_of_week"].astype(int)
    calendar_days = calendar_days.drop_duplicates()

    if not has_calendar:
        # Some merged feeds omit calendar.txt and keep service_ids in trips.txt
        # that have no calendar_dates rows. Keep these trips visible by treating
        # them as unknown-day services and including them in all day buckets.
        all_weekdays = pd.DataFrame(
            [(service_id, day_of_week) for service_id in tables["trips.txt"]["service_id"].astype(str).dropna().unique()
             for day_of_week in range(7)],
            columns=["service_id", "day_of_week"],
        )
        missing_service_ids = (
            set(all_weekdays["service_id"].unique())
            - set(calendar_days["service_id"].unique())
        )
        if missing_service_ids:
            logger.warning(
                "calendar.txt missing and %s service_ids have no calendar_dates rows; assigning all weekdays",
                len(missing_service_ids),
            )
            fallback_rows = all_weekdays[all_weekdays["service_id"].isin(missing_service_ids)]
            calendar_days = pd.concat([calendar_days, fallback_rows], ignore_index=True).drop_duplicates()

    logger.debug(f"Unique service_ids in calendar data: {len(calendar_days['service_id'].unique())}")
    logger.debug(f"Day of week counts: {calendar_days['day_of_week'].value_counts().to_dict()}")

    return SimpleNamespace(
        stop_times=stop_times,
        trips=tables["trips.txt"],
        routes=tables["routes.txt"],
        calendar=calendar_days,
    )


def _parse_gtfs_time_to_seconds(value: Any) -> int | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    parts = text.split(":")
    if len(parts) != 3:
        return None
    hours, minutes, seconds = parts
    return int(hours) * 3600 + int(minutes) * 60 + int(seconds)


def make_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Generate HTML and/or JSON timetables between two adjacent GTFS stops."
    )
    parser.add_argument("--gtfs-path", help="Path to GTFS zip/directory. Auto-detects when omitted.")
    parser.add_argument("--from-stop", default="ST_44120", help="Source stop_id (default: ST_44120).")
    parser.add_argument("--to-stop", default="ST_44121", help="Target stop_id (default: ST_44121).")
    parser.add_argument("--from-label", default="Doubravka", help="Human label used for output titles/files.")
    parser.add_argument("--to-label", default="Hlavní nádraží", help="Human label used for output titles/files.")
    parser.add_argument("--title", help="Custom HTML title for the forward direction.")
    parser.add_argument("--template-path", help="Optional Jinja2 template path.")
    parser.add_argument("--html-out", help="Forward HTML output path.")
    parser.add_argument("--json-out", help="Forward JSON output path.")
    parser.add_argument("--stdout-json", action="store_true", help="Print forward JSON timetable to stdout.")
    parser.add_argument("--reverse", action="store_true", help="Also generate reverse direction outputs.")
    parser.add_argument("--reverse-title", help="Custom HTML title for reverse direction.")
    parser.add_argument("--reverse-html-out", help="Reverse HTML output path.")
    parser.add_argument("--reverse-json-out", help="Reverse JSON output path.")
    return parser


def main() -> int:
    load_dotenv()
    parser = make_parser()
    args = parser.parse_args()
    delays_endpoint = (os.getenv("TIMETABLE_DELAYS_ENDPOINT") or "").strip() or None

    logger.debug(f"Starting with arguments: from_stop={args.from_stop}, to_stop={args.to_stop}")
    logger.debug(f"Labels: {args.from_label} -> {args.to_label}")

    gtfs_path = resolve_gtfs_path(args.gtfs_path)
    logger.debug(f"Resolved GTFS path: {gtfs_path}")

    template = Path(args.template_path).read_text(encoding="utf-8") if args.template_path else DEFAULT_TEMPLATE

    feed = load_gtfs_feed(gtfs_path)
    logger.debug(f"Feed loaded successfully")

    logger.info("Building forward timetable...")
    forward = build_timetable(feed, args.from_stop, args.to_stop)
    forward_title = args.title or f"{args.from_label} - {args.to_label}"

    forward_html_out = Path(args.html_out) if args.html_out else Path(default_output_name(args.from_label, args.to_label))
    if args.html_out or (not args.json_out and not args.stdout_json and not args.reverse):
        logger.debug(f"Writing forward HTML to: {forward_html_out}")
        write_text(forward_html_out, render_html(forward, template, forward_title, delays_endpoint))
        print(f"Wrote HTML: {forward_html_out}")

    if args.json_out:
        logger.debug(f"Writing forward JSON to: {args.json_out}")
        write_json(Path(args.json_out), forward)
        print(f"Wrote JSON: {args.json_out}")

    if args.stdout_json:
        print(json.dumps(forward, ensure_ascii=False, indent=2))

    if args.reverse:
        logger.info("Building reverse timetable...")
        reverse = build_timetable(feed, args.to_stop, args.from_stop)
        reverse_title = args.reverse_title or f"{args.to_label} - {args.from_label}"
        reverse_html_out = (
            Path(args.reverse_html_out)
            if args.reverse_html_out
            else Path(default_output_name(args.to_label, args.from_label))
        )

        logger.debug(f"Writing reverse HTML to: {reverse_html_out}")
        write_text(reverse_html_out, render_html(reverse, template, reverse_title, delays_endpoint))
        print(f"Wrote reverse HTML: {reverse_html_out}")

        if args.reverse_json_out:
            logger.debug(f"Writing reverse JSON to: {args.reverse_json_out}")
            write_json(Path(args.reverse_json_out), reverse)
            print(f"Wrote reverse JSON: {args.reverse_json_out}")

    logger.info("Completed successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
