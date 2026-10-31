"""Optional PIN-scoped partitions; use the original client for one partition."""
from contextlib import contextmanager
import re
import threading
import unicodedata
import logging
import time
import json
from urllib.parse import urlsplit
import requests

from .sigma_client import ActionNotAllowedError, SigmaClient

_LOGGER = logging.getLogger(__name__)
DIAGNOSTIC_BUILD = "1.2.5.post11"


class MissingZonesError(ValueError):
    """The correct partition returned a valid status but no readable zones."""


class ZonesSessionExpiredError(ValueError):
    """The zones request returned the login form."""


class SelectionSessionExpiredError(ValueError):
    """Partition selection returned a login form, including after reauthentication."""


class PartitionClient(SigmaClient):
    """Serialize PIN-scoped access and discard session state when switching PIN."""

    def __init__(self, *args, second_pin, **kwargs):
        super().__init__(*args, **kwargs)
        self.partition_pins = {"1": self.pin, "2": second_pin.strip()}
        self._context = threading.local()
        self._active_partition = None
        self._paths = {}
        self._diagnostic_times = {}
        self._status_sequence = 0
        self._status_counts = {}
        self._status_summary_times = {}
        self._status_suppressed = {}

    def _diagnostic_phase(self, phase):
        context = getattr(self._context, "status_diagnostic", None)
        if context is not None:
            context["phase"] = phase
            # A failed request must never inherit a previous response's HTTP 200.
            context["last_response"] = {}

    def _diagnostic_increment(self, name):
        context = getattr(self._context, "status_diagnostic", None)
        if context is not None:
            context[name] += 1

    def _diagnostic_issue(self, reason):
        context = getattr(self._context, "status_diagnostic", None)
        if context is not None:
            context["issues"].add(reason)

    @staticmethod
    def _exception_reason(error):
        # Exception messages can contain credentials, addresses or URLs.
        if isinstance(error, MissingZonesError):
            return "missing_zones"
        if isinstance(error, (SelectionSessionExpiredError, ZonesSessionExpiredError)):
            return "session_expired"
        if isinstance(error, requests.Timeout):
            return "timeout"
        if isinstance(error, requests.ConnectionError):
            return "connection_error"
        if isinstance(error, requests.RequestException):
            return "request_error"
        return "invalid_response" if isinstance(error, ValueError) else "unexpected_error"

    def _diagnose_status_result(self, outcome, reason):
        context = self._context.status_diagnostic
        counts = self._status_counts[self.target_partition]
        counts[outcome] += 1
        key = (self.target_partition, outcome)
        now = time.monotonic()
        previous = self._status_summary_times.get(key)
        if previous is not None and now - previous < 300:
            self._status_suppressed[key] = self._status_suppressed.get(key, 0) + 1
            return
        self._status_summary_times[key] = now
        summary = {
            "build": DIAGNOSTIC_BUILD,
            "operation_id": context["operation_id"],
            "requested_partition": self.target_partition,
            "outcome": outcome, "phase": context["phase"], "reason": reason,
            "zones_reads": context["zones_reads"],
            "selection_attempts": context["selection_attempts"],
            "login_attempts": context["login_attempts"],
            "zones_reconnects": context["zones_reconnects"],
            "issues": sorted(context["issues"]),
            "last_response": context["last_response"],
            "total_status_reads": counts["reads"],
            "total_recovered": counts["recovered"], "total_failed": counts["failed"],
            "suppressed_summaries": self._status_suppressed.pop(key, 0),
        }
        # Visible with HA's default logging; rate-limited even for successful recovery.
        _LOGGER.warning("SIGMA_STATUS %s", json.dumps(summary, ensure_ascii=True, sort_keys=True))

    @staticmethod
    def _page_kind(url):
        """Only emit fixed page identifiers, never a host, query or arbitrary path."""
        try:
            page = urlsplit(str(url or "")).path.rsplit("/", 1)[-1].lower()
        except ValueError:
            return "other"
        known = {"panel.html", "part.cgi", "zones.html", "login.html", "user.html",
                 "access.html", "ucode", "arm", "arm.html", "disarm", "disarm.html",
                 "stay", "stay.html"}
        return page if page in known else "other"

    @staticmethod
    def _auth_form_kind(soup):
        if not PartitionClient.is_login_page(soup):
            return "none"
        actions = {PartitionClient._page_kind(form.get("action", ""))
                   for form in soup.find_all("form")}
        if soup.find("input", {"name": "username"}) is not None:
            return "web_login"
        if "ucode" in actions:
            return "pin"
        return "auth_unknown"

    def _response_soup(self, response):
        soup = super()._response_soup(response)
        history = getattr(response, "history", ()) or ()
        soup.__dict__["_sigma_response_diagnostic"] = {
            "final_page": self._page_kind(getattr(response, "url", "")),
            "http_status": getattr(response, "status_code", None),
            "redirects": len(history),
            "redirect_pages": [self._page_kind(getattr(item, "url", "")) for item in history[:5]],
            "response_chars": len(response.text),
            "auth_form_kind": self._auth_form_kind(soup),
            "form_actions": [self._page_kind(form.get("action", ""))
                             for form in soup.find_all("form")[:5]],
        }
        context = getattr(self._context, "status_diagnostic", None)
        if context is not None and context["phase"] in ("panel", "partition_selection", "zones"):
            context["last_response"] = {
                **soup.__dict__["_sigma_response_diagnostic"],
                "returned_partition": self.partition_number(soup),
                "login_token_present": self.is_login_page(soup),
            }
        return soup

    def _diagnose_zones(self, soup, reason):
        """Log structure only, at most once per reason/partition every five minutes."""
        key = (self.target_partition, reason)
        now = time.monotonic()
        previous = self._diagnostic_times.get(key)
        if previous is not None and now - previous < 300:
            return
        self._diagnostic_times[key] = now
        tables = soup.find_all("table")
        structure = []
        for table in tables[:10]:
            rows = table.find_all("tr")
            counts = [len(row.find_all("td", recursive=False)) for row in rows]
            structure.append({
                "normaltable": "normaltable" in table.get("class", []),
                "rows": len(rows), "header_cells": len(table.find_all("th")),
                "rows_with_4plus_td": sum(count >= 4 for count in counts),
                "td_per_row_first_12": counts[:12],
            })
        title = soup.title.get_text(" ", strip=True).lower() if soup.title else ""
        title_kind = "zones" if "zone" in title else "panel" if "panel" in title else "login" if "login" in title else "other"
        summary = {
            "build": DIAGNOSTIC_BUILD, "reason": reason,
            "requested_partition": self.target_partition,
            "returned_partition": self.partition_number(soup),
            "title_kind": title_kind, "login_token_present": self.is_login_page(soup),
            "forms": len(soup.find_all("form")), "tables_total": len(tables),
            "table_structure": structure,
        }
        context = getattr(self._context, "status_diagnostic", None)
        if context is not None:
            summary["operation_id"] = context["operation_id"]
        summary.update(soup.__dict__.get("_sigma_response_diagnostic", {}))
        _LOGGER.warning("SIGMA_DIAG %s", json.dumps(summary, ensure_ascii=True, sort_keys=True))

    @property
    def target_partition(self):
        return getattr(self._context, "partition", "1")

    @contextmanager
    def partition_scope(self, partition):
        partition = str(partition)
        if partition not in self.partition_pins:
            raise ValueError("Partition is not configured")
        previous = self.target_partition
        self._context.partition = partition
        try:
            yield
        finally:
            self._context.partition = previous

    def logout(self):
        self._diagnostic_phase("logout")
        self._active_partition = None
        super().logout()

    def login(self):
        self._diagnostic_increment("login_attempts")
        self.pin = self.partition_pins[self.target_partition]
        self._active_partition = None
        super().login()
        self._active_partition = self.target_partition

    def _submit_login(self):
        self._diagnostic_phase("login")
        return super()._submit_login()

    def _submit_pin(self):
        self._diagnostic_phase("pin")
        return super()._submit_pin()

    @staticmethod
    def partition_number(soup):
        match = re.search(r"Τμήμα\s*(\d+)\s*(?:\([^)]*\))?\s*:",
                          soup.get_text(" ", strip=True))
        return match.group(1) if match else None

    @staticmethod
    def is_login_page(soup):
        return soup.find("input", {"name": "gen_input"}) is not None

    def _select_once(self):
        if not self._session_authenticated:
            self.login()
        elif self._active_partition != self.target_partition:
            # Some Ixion versions accept ucode but keep the previous partition.
            self.logout()
            self.login()
        self._diagnostic_phase("panel")
        panel = self._get_soup("panel.html")
        number = self.partition_number(panel)
        if number is not None:
            if number != self.target_partition:
                self._diagnostic_phase("partition_selection")
                selected = super().select_partition(self.target_partition)
                selected_number = self.partition_number(selected)
                if self.is_login_page(selected):
                    raise SelectionSessionExpiredError(f"Requested partition {self.target_partition}; panel returned {selected_number or 'no partition'}")
                if selected_number not in (None, self.target_partition):
                    raise ValueError(f"Requested partition {self.target_partition}; panel returned {selected_number or 'no partition'}")
                return selected
            # PIN-scoped Aeolus pages do not need part.cgi.
            return panel
        if self.is_login_page(panel):
            raise SelectionSessionExpiredError("Ixion session expired")
        # Preserve the original explicit partition-selection flow on other firmware.
        self._diagnostic_phase("partition_selection")
        selected = super().select_partition(self.target_partition)
        if self.is_login_page(selected):
            raise SelectionSessionExpiredError("Ixion session expired during partition selection")
        number = self.partition_number(selected)
        if number is not None and number != self.target_partition:
            raise ValueError("Panel returned a different partition")
        return selected

    def select_partition(self, part_id=None):
        """Recover once from PIN rejection, expired sessions or wrong selection."""
        try:
            self._diagnostic_increment("selection_attempts")
            return self._select_once()
        except Exception as error:
            self._diagnostic_issue(self._exception_reason(error))
            self.logout()
            self.login()
            self._diagnostic_increment("selection_attempts")
            return self._select_once()

    def parse_zones_html(self, soup):
        # Validate the authoritative zones page even when selection pages lack a header.
        if self.partition_number(soup) != self.target_partition:
            self._diagnose_zones(soup, "wrong_partition")
            raise ValueError(f"Status page did not confirm partition {self.target_partition}")
        try:
            data = super().parse_zones_html(soup)
        except Exception:
            self._diagnose_zones(soup, "parse_error")
            raise
        if data.get("battery_volt") is None:
            match = re.search(r"Μπαταρία\s*:\s*([\d.,]+)\s*Volt", soup.get_text(" ", strip=True), re.I)
            if match:
                data["battery_volt"] = float(match.group(1).replace(",", "."))
        try:
            self.validate_status(data)
        except MissingZonesError:
            self._diagnose_zones(soup, "missing_zones")
            raise
        except ValueError:
            self._diagnose_zones(soup, "unknown_status")
            raise
        return data

    @staticmethod
    def normalize_status(value):
        # Ixion mixes Latin and Greek lookalikes; accented Greek also occurs.
        text = unicodedata.normalize("NFD", str(value or "").upper())
        text = "".join(ch for ch in text if not unicodedata.combining(ch))
        text = text.translate(str.maketrans("ΑΒΕΖΗΙΚΜΝΟΡΤΥΧ", "ABEZHIKMNOPTYX"))
        return " ".join(text.split())

    def parse_alarm_status(self, raw_status):
        normalized = self.normalize_status(raw_status)
        tokens = normalized.split()
        automatic = "AUTO" in tokens
        if automatic:
            normalized = " ".join(token for token in tokens if token != "AUTO")
        for source in (
            "AΦOΠΛIΣMENO", "OΠΛIΣMENO ME ZΩNEΣ BYPASS", "OΠΛIΣMENO",
            "ΠEPIMETPIKH OΠΛIΣH ME ZΩNEΣ BYPASS", "ΠEPIMETPIKH OΠΛIΣH",
        ):
            if normalized == self.normalize_status(source):
                state, bypass = super().parse_alarm_status(source)
                # AUTO describes the arming origin, not which zones were bypassed.
                if automatic and bypass is False:
                    bypass = None
                return state, bypass
        return None, None

    @staticmethod
    def resolve_bypass(state, bypass, zones):
        if bypass is not None or state not in ("Armed", "Armed Perimeter"):
            return bypass
        flags = [PartitionClient._to_bool(zone.get("bypass")) for zone in zones]
        if any(flag is True for flag in flags):
            return True
        if flags and all(flag is False for flag in flags):
            return False
        return None

    @staticmethod
    def _to_bool(value):
        if isinstance(value, bool):
            return value
        return SigmaClient._to_bool(PartitionClient.normalize_status(value))

    def validate_status(self, data):
        missing = []
        if self.parse_alarm_status(data.get("alarm_status"))[0] is None:
            missing.append("alarm_status")
        if not data.get("zones"):
            missing.append("zones")
        if missing == ["zones"]:
            raise MissingZonesError(
                f"Partition {self.target_partition}: invalid zones; "
                f"alarm_status={str(data.get('alarm_status'))[:100]!r}"
            )
        if missing:
            status = str(data.get("alarm_status") or "missing")[:100]
            raise ValueError(f"Partition {self.target_partition}: invalid {','.join(missing)}; alarm_status={status!r}")

    def _read_selected_zones(self):
        """Allow the panel to settle; repeated reads never reselect the partition."""
        time.sleep(1.0)
        for attempt in range(3):
            try:
                self._diagnostic_phase("zones")
                self._diagnostic_increment("zones_reads")
                soup = self._get_soup("zones.html")
                if self.is_login_page(soup):
                    self._diagnose_zones(soup, "zones_session_expired")
                    raise ZonesSessionExpiredError("Ixion session expired while reading zones")
                # Every response still has to confirm the target partition.
                data = self.parse_zones_html(soup)
                if attempt:
                    _LOGGER.debug("Partition %s zones recovered on read %s/3",
                                  self.target_partition, attempt + 1)
                return data
            except MissingZonesError:
                self._diagnostic_issue("missing_zones")
                if attempt == 2:
                    raise
                _LOGGER.debug("Partition %s returned no zones; rereading only zones.html (%s/3)",
                              self.target_partition, attempt + 2)
                time.sleep((1.0, 2.0)[attempt])

    def try_zones_directly(self):
        """Return validated data or raise; do not trigger a third upstream login."""
        for recovery in range(2):
            try:
                self.select_partition()
                return self._read_selected_zones()
            except (MissingZonesError, ZonesSessionExpiredError, requests.RequestException) as error:
                self._diagnostic_issue(self._exception_reason(error))
                if recovery:
                    raise
                _LOGGER.debug("Partition %s: one fresh login before final zones-read batch",
                              self.target_partition)
                self._diagnostic_increment("zones_reconnects")
                self.logout()
                self.login()

    def safe_get_partition_status(self, partition):
        with self.partition_scope(partition):
            previous = getattr(self._context, "status_diagnostic", None)
            self._status_sequence += 1
            counts = self._status_counts.setdefault(self.target_partition,
                {"reads": 0, "recovered": 0, "failed": 0, "previous_failed": False})
            counts["reads"] += 1
            self._context.status_diagnostic = {
                "operation_id": self._status_sequence, "phase": "start",
                "zones_reads": 0, "selection_attempts": 0, "login_attempts": 0,
                "zones_reconnects": 0, "issues": set(), "last_response": {},
            }
            try:
                # Upstream still sends allowlisted analytics after a successful read.
                # try_zones_directly owns recovery and never returns None.
                data = super().safe_get_status()
            except Exception as error:
                reason = self._exception_reason(error)
                self._diagnostic_issue(reason)
                counts["previous_failed"] = True
                self._diagnose_status_result("failed", reason)
                raise
            else:
                context = self._context.status_diagnostic
                if counts["previous_failed"]:
                    context["issues"].add("previous_read_failed")
                if context["issues"]:
                    reason = "retry_succeeded" if context["issues"] != {"previous_read_failed"} else "subsequent_read_succeeded"
                    self._diagnose_status_result("recovered", reason)
                counts["previous_failed"] = False
                return data
            finally:
                self._context.status_diagnostic = previous

    def _parse_control_status(self, soup, zones_page=False):
        """Require a fresh target/state, independently of the zone table's readiness."""
        if self.is_login_page(soup):
            self._diagnose_zones(soup, "control_auth_required")
            raise SelectionSessionExpiredError("Ixion requested authentication for control state")
        if self.partition_number(soup) != self.target_partition:
            self._diagnose_zones(soup, "control_wrong_partition")
            raise ValueError(f"Control page did not confirm partition {self.target_partition}")
        data = SigmaClient.parse_zones_html(self, soup)
        if self.parse_alarm_status(data.get("alarm_status"))[0] is None:
            self._diagnose_zones(soup, "control_unknown_status")
            raise ValueError("Control page returned an unrecognized alarm state")
        # Tables on panel/access pages cannot establish fresh zone measurements.
        if not zones_page:
            data["zones"] = []
        return data

    def _publish_already_desired_status(self, raw):
        self._publish_action_status(raw)

    def _read_action_status(self):
        panel = self.select_partition()
        if self.partition_number(panel) is not None:
            return self._parse_control_status(panel)
        return self._parse_control_status(self._get_soup("zones.html"), zones_page=True)

    def _validate_action_state(self, action, data):
        state, _ = self.parse_alarm_status(data.get("alarm_status"))
        if action == "stay" and state == "Armed":
            raise ActionNotAllowedError(
                "Cannot change a fully armed partition to perimeter mode directly. "
                "Disarm it manually first when you want to change mode."
            )

    def _prepare_action(self, action):
        return self._action_paths(action=action)

    def _action_paths(self, action=None):
        """Read control links only for optional multi-partition operation."""
        panel = self.select_partition()
        direct = self.partition_number(panel) == self.target_partition
        soup = self._get_soup("access.html")
        number = self.partition_number(soup)
        if self.is_login_page(soup) or number is not None:
            data = self._parse_control_status(soup)
        else:
            data = self._parse_control_status(self._get_soup("zones.html"), zones_page=True)
        if action is not None:
            self._validate_action_state(action, data)
        paths = super()._action_paths()
        if direct:
            # Known PIN-scoped Aeolus firmware uses extensionless control paths.
            paths = {action: action for action in paths}
        for link in soup.find_all("a", href=True):
            url = urlsplit(link["href"])
            if url.netloc and url.netloc != urlsplit(self.base_url).netloc:
                continue
            if url.scheme and url.scheme != urlsplit(self.base_url).scheme:
                continue
            for action in paths:
                if url.path.lstrip("/") in (action, action + ".html") and not url.query and not url.fragment:
                    paths[action] = url.path.lstrip("/")
        self._paths[self.target_partition] = paths
        return paths

    def perform_partition_action(self, action, partition):
        with self.partition_scope(partition):
            # perform_action owns the shared lock, retries, re-authentication and polls.
            return super().perform_action(action)

    def _publish_action_data(self, data):
        data = dict(data)
        data["zones_available"] = bool(data.get("zones"))
        data["zones_bypassed"] = self.resolve_bypass(
            data.get("status"), data.get("zones_bypassed"), data.get("zones", [])
        )
        partition = self.target_partition
        self.coordinator.hass.loop.call_soon_threadsafe(
            self.coordinator.async_set_partition_data, partition, data
        )
