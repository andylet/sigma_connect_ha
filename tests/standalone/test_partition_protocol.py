"""Protocol regressions with a simulated Ixion; no private fixtures or network."""
import asyncio
import importlib.util
import json
import pathlib
import sys
import types
import unittest
import requests
from unittest.mock import patch

ROOT = pathlib.Path(__file__).resolve().parents[2]
PKG = "partition_protocol_test_pkg"
package = types.ModuleType(PKG)
package.__path__ = [str(ROOT / "custom_components" / "sigma_connect_ha")]
sys.modules[PKG] = package
client_module = __import__(PKG + ".partition_client", fromlist=["PartitionClient"])
base_module = __import__(PKG + ".sigma_client", fromlist=["SigmaClient"])
PartitionClient = client_module.PartitionClient
from bs4 import BeautifulSoup


def html(part, kind="zones", state="AΦOΠΛIΣMENO"):
    header = f"<p>Τμήμα {part}: {state}</p><div>Μπαταρία: 13.7 Volt</div><div>Παροχή 230V: NAI</div>"
    if kind == "zones":
        return header + '<table class="normaltable"><tr><th>Zone</th></tr><tr><td>21</td><td>Example</td><td>Κλειστή</td><td>OXI</td></tr></table>'
    if kind == "access":
        return header + '<a href="/arm">Arm</a><a href="/disarm">Disarm</a><a href="/stay">Stay</a>'
    return header + '<a href="zones.html">Κατάσταση ζωνών</a>'


class Response:
    def __init__(self, text, path="zones.html"):
        self.text = text
        self.url = "http://alarm.example:5053/" + path
        self.status_code = 200

    def raise_for_status(self):
        pass


class Ixion:
    def __init__(self):
        self.auth = False
        self.part = "1"
        self.states = {"1": "AΦOΠΛIΣMENO", "2": "AΦOΠΛIΣMENO"}
        self.logins = 0
        self.pin_submissions = 0
        self.expire_on_command = False
        self.reject_pin_once = False
        self.wrong_zones = False
        self.commands = []
        self.explicit_selection = False
        self.cookies = {}

    def close(self):
        pass

    def get(self, url, **kwargs):
        path = url.rsplit("/", 1)[-1]
        if path == "logout.html":
            self.auth = False
            return Response("")
        if path == "login.html" or not self.auth:
            return Response('<input name="gen_input" value="abcdefghijklmnop">', "login.html")
        if path == "user.html":
            return Response('<input name="gen_input" value="abcdefghijklmnop">', path)
        if path in ("arm", "disarm", "stay", "arm.html", "disarm.html", "stay.html"):
            if self.expire_on_command:
                self.expire_on_command = False
                self.auth = False
                return Response('<input name="gen_input" value="abcdefghijklmnop">', "login.html")
            action = path.split(".")[0]
            self.commands.append((self.part, action))
            self.states[self.part] = {"arm": "OΠΛIΣMENO", "disarm": "AΦOΠΛIΣMENO", "stay": "ΠEPIMETPIKH OΠΛIΣH"}[action]
        if path == "panel.html" and self.explicit_selection:
            return Response('<form action="part.cgi"></form>', path)
        kind = "zones" if path == "zones.html" else "access" if path == "access.html" else "panel"
        part = ("1" if self.part == "2" else "2") if self.wrong_zones and kind == "zones" else self.part
        return Response(html(part, kind, self.states[self.part]), path)

    def post(self, url, data, **kwargs):
        path = url.rsplit("/", 1)[-1]
        if path == "login.html":
            self.logins += 1
            self.auth = True
        elif path == "ucode":
            self.pin_submissions += 1
            if self.reject_pin_once:
                self.reject_pin_once = False
                self.auth = False
            else:
                self.part = "1" if data["password"] == "pin-one" else "2"
        elif path == "part.cgi":
            self.part = data["part"].replace("part", "")
        return Response(html(self.part, "panel", self.states[self.part]), path)


class ObservedLock(asyncio.Lock):
    """Signal once an action is actually queued behind the current owner."""
    def __init__(self):
        super().__init__()
        self.waiter_started = asyncio.Event()

    async def acquire(self):
        if self.locked():
            self.waiter_started.set()
        return await super().acquire()


class ProtocolTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.panel = Ixion()
        loop = asyncio.get_running_loop()
        self.coordinator = types.SimpleNamespace(
            lock=asyncio.Lock(), hass=types.SimpleNamespace(loop=loop),
            updates=[], async_set_partition_data=lambda p, d: self.coordinator.updates.append((p, d)),
        )
        self.client = PartitionClient("http://alarm.example:5053", "example", "example", "1111",
                                      self.coordinator, second_pin="2222", send_analytics=False)
        self.client.session.close()
        self.client.session = self.panel
        self.client._create_session = lambda: self.panel
        self.client._encrypt = lambda secret, token: ("pin-one" if secret == "1111" else "pin-two", "1")
        self.sleep_patch = patch(PKG + ".sigma_client.time.sleep")
        self.sleep_patch.start()
        self.addCleanup(self.sleep_patch.stop)

    def test_partition_switch_discards_old_session(self):
        for part in ("1", "2", "2", "1"):
            data = self.client.safe_get_partition_status(part)
            self.assertEqual(data["battery_volt"], 13.7)
        self.assertEqual(self.panel.logins, 3)
        self.assertEqual(self.panel.pin_submissions, 3)

    def test_expired_session_recovers(self):
        self.client.safe_get_partition_status("1")
        self.panel.auth = False
        self.client.safe_get_partition_status("1")
        self.assertEqual(self.panel.logins, 2)

    def test_pin_switch_failure_recovers_with_login(self):
        self.client.safe_get_partition_status("1")
        self.panel.reject_pin_once = True
        self.client.safe_get_partition_status("2")
        self.assertEqual(self.panel.part, "2")
        self.assertEqual(self.panel.logins, 3)

    def test_explicit_selection_without_header(self):
        self.panel.explicit_selection = True
        self.client.safe_get_partition_status("2")
        self.assertEqual(self.panel.part, "2")

    def test_wrong_zones_rejected(self):
        self.panel.wrong_zones = True
        with self.assertRaises(ValueError):
            self.client.safe_get_partition_status("2")
        self.assertEqual(self.panel.commands, [])

    async def test_actions_reuse_upstream_loop_and_preserve_other_partition(self):
        ok = await asyncio.to_thread(self.client.perform_partition_action, "arm",…2698 tokens truncated…    self.panel.get, self.panel.post = get, post
        with patch.object(self.panel, "get", wraps=self.panel.get) as calls:
            self.assertTrue(await asyncio.to_thread(self.client.perform_partition_action, "arm", "2"))
        await asyncio.sleep(0)
        paths = [call.args[0].rsplit("/", 1)[-1] for call in calls.call_args_list]
        self.assertEqual(paths.count("zones.html"), 3)
        self.assertEqual(self.panel.commands, [("2", "arm")])
        published = self.coordinator.updates[-1][1]
        self.assertEqual(published["status"], "Armed")
        self.assertTrue(published["zones"])
        self.assertIs(published["zones_available"], True)
        self.assertFalse(self.coordinator.lock.locked())

    async def test_action_lock_timeout_cancels_waiter_without_unlocking_another_owner(self):
        lock = ObservedLock()
        self.coordinator.lock = lock
        await lock.acquire()
        self.client.ACTION_LOCK_TIMEOUT = 0.02
        with patch.object(self.panel, "get", wraps=self.panel.get) as get:
            task = asyncio.create_task(asyncio.to_thread(self.client.perform_partition_action, "arm", "2"))
            await asyncio.wait_for(lock.waiter_started.wait(), timeout=1)
            self.assertFalse(await asyncio.wait_for(task, timeout=2))
            get.assert_not_called()
        self.assertTrue(lock.locked())  # The original owner retains its lock.
        self.assertEqual(self.panel.commands, [])
        self.assertEqual(self.panel.logins, 0)
        lock.release()  # Release strictly after the waiter's deadline has expired.
        # An abandoned acquisition would consume this release and strand the next owner.
        await asyncio.wait_for(lock.acquire(), timeout=1)
        await asyncio.sleep(0)
        self.assertTrue(lock.locked())  # The timed-out action cannot release this third owner.
        self.assertEqual(self.panel.commands, [])
        lock.release()
        self.client.ACTION_LOCK_TIMEOUT = 1
        self.assertTrue(await asyncio.wait_for(
            asyncio.to_thread(self.client.perform_partition_action, "arm", "2"), timeout=2))
        await asyncio.sleep(0)
        self.assertEqual(self.panel.commands, [("2", "arm")])
        self.assertFalse(lock.locked())

    async def test_action_lock_owner_release_before_deadline_runs_waiting_action(self):
        lock = ObservedLock()
        self.coordinator.lock = lock
        await lock.acquire()
        self.client.ACTION_LOCK_TIMEOUT = 1
        with patch.object(self.panel, "get", wraps=self.panel.get) as get:
            task = asyncio.create_task(asyncio.to_thread(self.client.perform_partition_action, "stay", "2"))
            await asyncio.wait_for(lock.waiter_started.wait(), timeout=1)
            get.assert_not_called()
            self.assertEqual(self.panel.commands, [])
            lock.release()  # The loop-owned waiter is still within its deadline.
            self.assertTrue(await asyncio.wait_for(task, timeout=2))
        await asyncio.sleep(0)
        self.assertEqual(self.panel.commands, [("2", "stay")])
        self.assertFalse(lock.locked())

    async def test_stay_from_fully_armed_variants_is_nonretryable_and_sends_no_command(self):
        other_partition = self.panel.states["1"]
        for state in ("\u039f\u03a0\u039b\u0399\u03a3\u039c\u0395\u039d\u039f",
                      "\u039f\u03a0\u039b\u0399\u03a3\u039c\u0395\u039d\u039f AUTO",
                      "\u039f\u03a0\u039b\u0399\u03a3\u039c\u0395\u039d\u039f \u039c\u0395 \u0396\u03a9\u039d\u0395\u03a3 BYPASS"):
            with self.subTest(state=state):
                self.panel.states["2"] = state
                with patch.object(self.panel, "get", wraps=self.panel.get) as get, \
                        patch(PKG + ".sigma_client.MAX_ACTION_ATTEMPTS", 5):
                    with self.assertRaises(base_module.ActionNotAllowedError):
                        await asyncio.to_thread(self.client.perform_partition_action, "stay", "2")
                await asyncio.sleep(0)
                paths = [call.args[0].rsplit("/", 1)[-1] for call in get.call_args_list]
                self.assertEqual(paths.count("panel.html"), 1)
                self.assertEqual(paths.count("zones.html"), 0)
                self.assertNotIn("access.html", paths)
                self.assertEqual(self.panel.commands, [])
                self.assertEqual(self.panel.states["2"], state)
                self.assertEqual(self.panel.states["1"], other_partition)
                self.assertFalse(self.coordinator.lock.locked())
        self.assertEqual(self.coordinator.updates, [])

    async def test_stay_already_perimeter_returns_success_without_a_command(self):
        self.panel.states["2"] = "\u03a0\u0395\u03a1\u0399\u039c\u0395\u03a4\u03a1\u0399\u039a\u0397 \u039f\u03a0\u039b\u0399\u03a3\u0397"
        states_before = self.panel.states.copy()
        with patch.object(self.panel, "get", wraps=self.panel.get) as get:
            self.assertTrue(await asyncio.to_thread(self.client.perform_partition_action, "stay", "2"))
        await asyncio.sleep(0)
        paths = [call.args[0].rsplit("/", 1)[-1] for call in get.call_args_list]
        self.assertEqual(paths.count("zones.html"), 0)
        self.assertNotIn("access.html", paths)
        self.assertEqual(self.panel.commands, [])
        self.assertEqual(self.panel.states, states_before)
        self.assertEqual(self.coordinator.updates[-1][1]["status"], "Armed Perimeter")
        self.assertEqual(self.coordinator.updates[-1][1]["zones"], [])
        self.assertIs(self.coordinator.updates[-1][1]["zones_available"], False)
        self.assertFalse(self.coordinator.lock.locked())

    async def test_stay_from_disarmed_still_sends_one_partition_two_command(self):
        other_partition = self.panel.states["1"]
        self.assertTrue(await asyncio.to_thread(self.client.perform_partition_action, "stay", "2"))
        await asyncio.sleep(0)
        self.assertEqual(self.panel.commands, [("2", "stay")])
        self.assertEqual(self.panel.states["1"], other_partition)
        self.assertEqual(self.coordinator.updates[-1][0], "2")
        self.assertEqual(self.coordinator.updates[-1][1]["status"], "Armed Perimeter")
        self.assertFalse(self.coordinator.lock.locked())

    async def test_external_full_arming_before_preflight_blocks_stay_without_retries(self):
        original = self.panel.get
        reads = 0
        accesses = 0
        armed = "\u039f\u03a0\u039b\u0399\u03a3\u039c\u0395\u039d\u039f"
        other_partition = self.panel.states["1"]
        def get(url, **kwargs):
            nonlocal reads, accesses
            if url.endswith("/zones.html"):
                reads += 1
            if url.endswith("/access.html"):
                accesses += 1
                self.panel.states["2"] = armed
            return original(url, **kwargs)
        self.panel.get = get
        with patch(PKG + ".sigma_client.MAX_ACTION_ATTEMPTS", 5):
            with self.assertRaises(base_module.ActionNotAllowedError):
                await asyncio.to_thread(self.client.perform_partition_action, "stay", "2")
        await asyncio.sleep(0)
        self.assertEqual(reads, 0)  # Initial and preflight states came from fresh control headers.
        self.assertEqual(accesses, 1)
        self.assertEqual(self.panel.logins, 1)
        self.assertEqual(self.panel.commands, [])
        self.assertEqual(self.panel.states["2"], armed)
        self.assertEqual(self.panel.states["1"], other_partition)
        self.assertEqual(self.coordinator.updates, [])
        self.assertFalse(self.coordinator.lock.locked())

    async def test_external_full_arming_on_preflight_panel_blocks_stay_without_command(self):
        original = self.panel.get
        panel_reads = 0
        armed = "\u039f\u03a0\u039b\u0399\u03a3\u039c\u0395\u039d\u039f"
        def get(url, **kwargs):
            nonlocal panel_reads
            if url.endswith("/panel.html"):
                panel_reads += 1
                if panel_reads == 2:
                    self.panel.states["2"] = armed
            return original(url, **kwargs)
        self.panel.get = get
        with patch(PKG + ".sigma_client.MAX_ACTION_ATTEMPTS", 5):
            with self.assertRaises(base_module.ActionNotAllowedError):
                await asyncio.to_thread(self.client.perform_partition_action, "stay", "2")
        await asyncio.sleep(0)
        self.assertEqual(panel_reads, 2)
        self.assertEqual(self.panel.commands, [])
        self.assertEqual(self.coordinator.updates, [])
        self.assertFalse(self.coordinator.lock.locked())

    def test_control_parser_never_claims_zone_rows_from_panel_or_access(self):
        # The same valid rows are useful only when fetched from the actual zones endpoint.
        soup = BeautifulSoup(html("2"), "html.parser")
        with self.client.partition_scope("2"):
            control = self.client._parse_control_status(soup)
            zones = self.client._parse_control_status(soup, zones_page=True)
        self.assertEqual(self.client.parse_alarm_status(control["alarm_status"])[0], "Disarmed")
        self.assertEqual(control["zones"], [])
        self.assertTrue(zones["zones"])

    def test_authentication_form_diagnostics_classify_known_forms_without_secrets(self):
        cases = [
            ("web_login", "login.html", '<input name="username" value="PRIVATE_USER"><input name="password" value="PRIVATE_PASS"><input name="gen_input" value="PRIVATE_TOKEN">'),
            ("pin", "ucode", '<input name="password" value="PRIVATE_PIN"><input name="gen_input" value="PRIVATE_TOKEN">'),
            ("auth_unknown", "PRIVATE_ACTION", '<input name="gen_input" value="PRIVATE_TOKEN">'),
            ("auth_unknown", "http://[", '<input name="gen_input" value="PRIVATE_TOKEN">'),
            ("none", "access.html", '<input name="other" value="PRIVATE_OTHER">'),
        ]
        for kind, action, inputs in cases:
            with self.subTest(kind=kind):
                url = action if action == "http://[" else "http://PRIVATE_USER:PRIVATE_PASSWORD@PRIVATE_HOST/" + action + "?token=PRIVATE_QUERY"
                page = '<form action="' + url + '">' + inputs + '</form>'
                soup = self.client._response_soup(Response(page, "zones.html"))
                diagnostic = soup.__dict__["_sigma_response_diagnostic"]
                self.assertEqual(diagnostic["auth_form_kind"], kind)
                self.assertEqual(diagnostic["form_actions"], ["other" if action in ("PRIVATE_ACTION", "http://[") else action])
                self.assertNotIn("PRIVATE", json.dumps(diagnostic))
                self.assertNotIn("http://[", json.dumps(diagnostic))

    async def test_valid_control_headers_do_not_request_wrong_unused_zones(self):
        self.panel.wrong_zones = True
        with patch.object(self.panel, "get", wraps=self.panel.get) as calls:
            self.assertTrue(await asyncio.to_thread(self.client.perform_partition_action, "arm", "2"))
        await asyncio.sleep(0)
        self.assertEqual(self.panel.commands, [("2", "arm")])
        self.assertFalse(any(call.args[0].endswith("/zones.html") for call in calls.call_args_list))
        self.assertFalse(self.coordinator.lock.locked())

    async def test_concurrent_actions_are_serialized(self):
        results = await asyncio.gather(
            asyncio.to_thread(self.client.perform_partition_action, "arm", "1"),
            asyncio.to_thread(self.client.perform_partition_action, "stay", "2"),
        )
        self.assertTrue(all(results))
        self.assertCountEqual(self.panel.commands, [("1", "arm"), ("2", "stay")])
        self.assertFalse(self.coordinator.lock.locked())

    def test_analytics_does_not_send_partition_pins(self):
        with patch(PKG + ".sigma_client.requests.post") as post:
            base_module.post_installation_analytics("http://alarm.example", {"pin": "PIN_SECRET_ONE", "partition_2_pin": "PIN_SECRET_TWO", "update_interval": 60})
        payload = post.call_args.kwargs["json"]
        self.assertNotIn("PIN_SECRET", str(payload))
        self.assertEqual(payload["config"]["update_interval"], 60)

    def test_greek_and_mixed_status_variants(self):
        cases = [
            ("ΑΦΟΠΛΙΣΜΕΝΟ", ("Disarmed", None)),
            ("ΟΠΛΙΣΜΕΝΟ", ("Armed", False)),
            ("ΟΠΛΙΣΜΕΝΟ ΜΕ ΖΩΝΕΣ BYPASS", ("Armed", True)),
            ("ΠΕΡΙΜΕΤΡΙΚΗ ΟΠΛΙΣΗ", ("Armed Perimeter", False)),
            ("ΠΕΡΙΜΕΤΡΙΚΗ ΟΠΛΙΣΗ ΜΕ ΖΩΝΕΣ BYPASS", ("Armed Perimeter", True)),
            ("  Όπλισμένο   με  ζώνες BYPASS ", ("Armed", True)),
            ("OΠΛIΣMENO", ("Armed", False)),
        ]
        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.assertEqual(self.client.parse_alarm_status(raw), expected)

    def test_unknown_state_reports_which_field_failed(self):
        with self.client.partition_scope("2"):
            with self.assertRaisesRegex(ValueError, "invalid alarm_status"):
                self.client.parse_zones_html(BeautifulSoup(html("2", state="UNRECOGNIZED STATE"), "html.parser"))

    def test_missing_battery_does_not_invalidate_alarm(self):
        with self.client.partition_scope("2"):
            page = html("2").replace("<div>Μπαταρία: 13.7 Volt</div>", "")
            result = self.client.parse_zones_html(BeautifulSoup(page, "html.parser"))
            self.assertIsNone(result["battery_volt"])
            self.assertTrue(result["zones"])

    def test_comma_decimal_battery_and_greek_no(self):
        with self.client.partition_scope("2"):
            result = self.client.parse_zones_html(BeautifulSoup(html("2").replace("13.7", "13,7"), "html.parser"))
            self.assertEqual(result["battery_volt"], 13.7)
            self.assertIs(self.client._to_bool("ΟΧΙ"), False)

    def test_missing_zones_reports_field(self):
        with self.client.partition_scope("2"):
            with self.assertRaisesRegex(ValueError, "invalid zones"):
                self.client.parse_zones_html(BeautifulSoup(html("2", kind="panel"), "html.parser"))

    def test_switch_resets_server_that_ignores_pin_changes(self):
        original = self.panel.post
        authenticated_part = None
        def post(url, data, **kwargs):
            nonlocal authenticated_part
            if url.endswith("login.html"):
                authenticated_part = None
            if url.endswith("ucode") and authenticated_part is not None:
                return Response(html(authenticated_part, "panel"))
            response = original(url, data, **kwargs)
            if url.endswith("ucode"):
                authenticated_part = self.panel.part
            return response
        self.panel.post = post
        self.client.safe_get_partition_status("1")
        self.client.safe_get_partition_status("2")
        self.assertEqual(self.panel.part, "2")
        self.assertEqual(self.panel.logins, 2)

    def test_greek_armed_status_does_not_cause_endless_failure(self):
        self.panel.states["2"] = "ΟΠΛΙΣΜΕΝΟ ΜΕ ΖΩΝΕΣ BYPASS"
        for part in ("1", "2", "2", "1", "1", "2"):
            result = self.client.safe_get_partition_status(part)
            self.assertIsNotNone(self.client.parse_alarm_status(result["alarm_status"])[0])


    def test_auto_armed_variants(self):
        for raw in ("ΟΠΛΙΣΜΕΝΟ AUTO", "OΠΛIΣMENO AUTO", "  Όπλισμένο auto  "):
            with self.subTest(raw=raw):
                self.assertEqual(self.client.parse_alarm_status(raw), ("Armed", None))

    def test_auto_bypass_is_read_from_zones(self):
        resolve = self.client.resolve_bypass
        self.assertIs(resolve("Armed", None, [{"bypass": "ΟΧΙ"}]), False)
        self.assertIs(resolve("Armed", None, [{"bypass": "NAI"}]), True)
        self.assertIsNone(resolve("Armed", None, [{"bypass": "unknown"}]))
        self.assertIsNone(resolve("Armed", None, []))
        self.assertIs(resolve("Armed", None, [{"bypass": False}]), False)

    def test_auto_unknown_state_still_rejected(self):
        self.assertEqual(self.client.parse_alarm_status("UNRECOGNIZED AUTO"), (None, None))

    async def test_automatic_arming_then_remote_disarming(self):
        self.panel.states["2"] = "ΟΠΛΙΣΜΕΝΟ AUTO"
        result = self.client.safe_get_partition_status("2")
        self.assertEqual(self.client.parse_alarm_status(result["alarm_status"])[0], "Armed")
        self.assertTrue(await asyncio.to_thread(self.client.perform_partition_action, "disarm", "2"))
        await asyncio.sleep(0)
        self.assertEqual(self.coordinator.updates[-1][1]["status"], "Disarmed")
        self.assertEqual(self.panel.commands, [("2", "disarm")])


    def test_missing_zones_recovers_with_bounded_rereads(self):
        original = self.panel.get
        reads = 0
        selections = 0
        def get(url, **kwargs):
            nonlocal reads, selections
            if url.endswith("/panel.html"):
                selections += 1
            if url.endswith("/zones.html"):
                reads += 1
                if reads <= 2:
                    return Response(html(self.panel.part, kind="panel"))
            return original(url, **kwargs)
        self.panel.get = get
        data = self.client.safe_get_partition_status("2")
        self.assertTrue(data["zones"])
        self.assertEqual(reads, 3)
        self.assertEqual(selections, 1)
        self.assertEqual(self.panel.logins, 1)
        self.assertEqual(self.panel.commands, [])
        self.assertEqual(base_module.time.sleep.call_args_list[-2:],
                         [unittest.mock.call(1.0), unittest.mock.call(2.0)])

    def test_persistent_missing_zones_stops_after_two_batches(self):
        original = self.panel.get
        reads = 0
        def get(url, **kwargs):
            nonlocal reads
            if url.endswith("/zones.html"):
                reads += 1
                return Response(html(self.panel.part, kind="panel"))
            return original(url, **kwargs)
        self.panel.get = get
        with self.assertRaises(client_module.MissingZonesError):
            self.client.safe_get_partition_status("2")
        self.assertEqual(reads, 6)
        self.assertEqual(self.panel.logins, 2)
        self.assertEqual(self.panel.commands, [])

    def test_wrong_partition_is_not_treated_as_missing_zones(self):
        original = self.panel.get
        reads = 0
        def get(url, **kwargs):
            nonlocal reads
            if url.endswith("/zones.html"):
                reads += 1
                return Response(html("1", kind="panel"))
            return original(url, **kwargs)
        self.panel.get = get
        with self.assertRaisesRegex(ValueError, "did not confirm partition 2"):
            self.client.safe_get_partition_status("2")
        self.assertEqual(reads, 1)
        self.assertEqual(self.panel.commands, [])

    def test_unknown_status_without_zones_is_not_retried_as_empty_zones(self):
        original = self.panel.get
        reads = 0
        def get(url, **kwargs):
            nonlocal reads
            if url.endswith("/zones.html"):
                reads += 1
                return Response(html("2", kind="panel", state="UNRECOGNIZED STATE"))
            return original(url, **kwargs)
        self.panel.get = get
        with self.assertRaisesRegex(ValueError, "invalid alarm_status,zones"):
            self.client.safe_get_partition_status("2")
        self.assertEqual(reads, 1)


    def test_diagnostics_capture_structure_without_private_content(self):
        page = html("2", kind="panel") + '<title>PRIVATE_TITLE</title><table class="PRIVATE_CLASS"><tr><td>PRIVATE_ZONE</td><td>PRIVATE_PIN</td><td>Closed</td><td>No</td></tr></table><input name="gen_input" value="PRIVATE_TOKEN">'
        response = Response(page)
        response.url = "http://PRIVATE_USER:PRIVATE_PASS@private.example/PRIVATE_PATH?token=PRIVATE_QUERY"
        response.history = [Response("", "login.html")]
        soup = self.client._response_soup(response)
        with self.client.partition_scope("2"), patch(PKG + ".partition_client._LOGGER.warning") as warning:
            with self.assertRaises(client_module.MissingZonesError):
                self.client.parse_zones_html(soup)
        encoded = warning.call_args.args[1]
        import json
        diag = json.loads(encoded)
        self.assertEqual(diag["reason"], "missing_zones")
        self.assertEqual(diag["final_page"], "other")
        self.assertEqual(diag["returned_partition"], "2")
        self.assertEqual(diag["tables_total"], 1)
        self.assertEqual(diag["table_structure"][0]["rows_with_4plus_td"], 1)
        self.assertEqual(diag["http_status"], 200)
        self.assertEqual(diag["redirects"], 1)
        self.assertNotIn("PRIVATE", encoded)
        self.assertNotIn("private.example", encoded)

    def test_diagnostics_are_rate_limited(self):
        soup = BeautifulSoup(html("2", kind="panel"), "html.parser")
        with self.client.partition_scope("2"), patch(PKG + ".partition_client._LOGGER.warning") as warning, patch(PKG + ".partition_client.time.monotonic", side_effect=[10, 11, 311]):
            for _ in range(3):
                with self.assertRaises(client_module.MissingZonesError):
                    self.client.parse_zones_html(soup)
        self.assertEqual(warning.call_count, 2)

    def test_diagnostics_capture_all_upstream_response_paths(self):
        original = self.panel.get
        reads = 0
        def get(url, **kwargs):
            nonlocal reads
            if url.endswith("/zones.html"):
                reads += 1
                return Response(html("2", kind="panel"), "zones.html")
            return original(url, **kwargs)
        self.panel.get = get
        with patch(PKG + ".partition_client._LOGGER.warning") as warning:
            with self.assertRaises(client_module.MissingZonesError):
                self.client.safe_get_partition_status("2")
        diagnostics = [json.loads(call.args[1]) for call in warning.call_args_list
                       if call.args[0] == "SIGMA_DIAG %s"]
        self.assertEqual(diagnostics[0]["final_page"], "zones.html")
        self.assertEqual(reads, 6)
        self.assertEqual(len(diagnostics), 1)

    def test_valid_status_does_not_log_diagnostics(self):
        with patch(PKG + ".partition_client._LOGGER.warning") as warning:
            self.client.safe_get_partition_status("2")
        warning.assert_not_called()


    def test_waits_for_ready_table_without_resetting_selection(self):
        original = self.panel.get
        clock = 0.0
        ready_at = 0.0
        events = []
        def sleep(seconds):
            nonlocal clock
            clock += seconds
            events.append(("wait", seconds))
        base_module.time.sleep.side_effect = sleep
        def get(url, **kwargs):
            nonlocal ready_at
            if url.endswith("/panel.html"):
                ready_at = clock + 2.0
                events.append(("select", self.panel.part))
            if url.endswith("/zones.html"):
                events.append(("zones", self.panel.part))
                if clock < ready_at:
                    return Response(html(self.panel.part, kind="panel"), "zones.html")
            return original(url, **kwargs)
        self.panel.get = get
        data = self.client.safe_get_partition_status("2")
        self.assertTrue(data["zones"])
        self.assertEqual(events, [("select", "2"), ("wait", 1.0), ("zones", "2"),
                                  ("wait", 1.0), ("zones", "2")])
        self.assertEqual(self.panel.logins, 1)
        self.assertEqual(self.panel.commands, [])

    def test_reconnects_once_after_empty_batch_then_recovers(self):
        original = self.panel.get
        reads = 0
        selections = 0
        def get(url, **kwargs):
            nonlocal reads, selections
            if url.endswith("/panel.html"):
                selections += 1
            if url.endswith("/zones.html"):
                reads += 1
                if self.panel.logins == 1:
                    return Response(html(self.panel.part, kind="panel"), "zones.html")
            return original(url, **kwargs)
        self.panel.get = get
        self.assertTrue(self.client.safe_get_partition_status("2")["zones"])
        self.assertEqual(reads, 4)
        self.assertEqual(selections, 2)
        self.assertEqual(self.panel.logins, 2)
        self.assertEqual(self.panel.commands, [])

    def test_wrong_partition_during_zones_retry_is_rejected(self):
        original = self.panel.get
        reads = 0
        def get(url, **kwargs):
            nonlocal reads
            if url.endswith("/zones.html"):
                reads += 1
                return Response(html("2" if reads == 1 else "1", kind="panel"), "zones.html")
            return original(url, **kwargs)
        self.panel.get = get
        with self.assertRaisesRegex(ValueError, "did not confirm partition 2"):
            self.client.safe_get_partition_status("2")
        self.assertEqual(reads, 2)
        self.assertEqual(self.panel.logins, 1)
        self.assertEqual(self.panel.commands, [])

    def test_unknown_status_during_retry_is_rejected(self):
        original = self.panel.get
        reads = 0
        def get(url, **kwargs):
            nonlocal reads
            if url.endswith("/zones.html"):
                reads += 1
                return Response(html("2", kind="panel", state="AΦOΠΛIΣMENO" if reads == 1 else "UNRECOGNIZED"), "zones.html")
            return original(url, **kwargs)
        self.panel.get = get
        with self.assertRaisesRegex(ValueError, "invalid alarm_status,zones"):
            self.client.safe_get_partition_status("2")
        self.assertEqual(reads, 2)
        self.assertEqual(self.panel.logins, 1)

    def test_expired_session_during_retry_gets_one_fresh_login(self):
        original = self.panel.get
        reads = 0
        def get(url, **kwargs):
            nonlocal reads
            if url.endswith("/zones.html"):
                reads += 1
                if reads == 1:
                    return Response(html("2", kind="panel"), "zones.html")
                if reads == 2:
                    self.panel.auth = False
                    return Response('<input name="gen_input" value="example">', "login.html")
            return original(url, **kwargs)
        self.panel.get = get
        self.assertTrue(self.client.safe_get_partition_status("2")["zones"])
        self.assertEqual(reads, 3)
        self.assertEqual(self.panel.logins, 2)
        self.assertEqual(self.panel.commands, [])

    def test_analytics_still_sends_once_after_zone_recovery(self):
        original = self.panel.get
        reads = 0
        def get(url, **kwargs):
            nonlocal reads
            if url.endswith("/zones.html"):
                reads += 1
                if reads == 1:
                    return Response(html("2", kind="panel"), "zones.html")
            return original(url, **kwargs)
        self.panel.get = get
        self.client._send_analytics = True
        self.client._config.update({"pin": "SECRET_ONE", "partition_2_pin": "SECRET_TWO"})
        with patch(PKG + ".sigma_client.requests.post") as post:
            self.client.safe_get_partition_status("2")
            self.client.safe_get_partition_status("2")
        post.assert_called_once()
        self.assertNotIn("SECRET", str(post.call_args.kwargs["json"]))

    @staticmethod
    def status_summaries(warning):
        return [json.loads(call.args[1]) for call in warning.call_args_list
                if call.args[0] == "SIGMA_STATUS %s"]

    def test_status_summary_healthy_operations_are_silent(self):
        with patch(PKG + ".partition_client._LOGGER.warning") as warning:
            for part in ("2", "2", "1"):
                self.assertTrue(self.client.safe_get_partition_status(part)["zones"])
        warning.assert_not_called()
        self.assertEqual(self.panel.commands, [])

    def test_status_summary_correlates_recovered_zones_and_diagnostic(self):
        original = self.panel.get
        reads = 0
        def get(url, **kwargs):
            nonlocal reads
            if url.endswith("/zones.html"):
                reads += 1
                if reads == 1:
                    return Response(html("2", kind="panel"), "zones.html")
            return original(url, **kwargs)
        self.panel.get = get
        with patch(PKG + ".partition_client._LOGGER.warning") as warning:
            self.assertTrue(self.client.safe_get_partition_status("2")["zones"])
        summaries = self.status_summaries(warning)
        self.assertEqual(len(summaries), 1)
        summary = summaries[0]
        self.assertEqual(summary["build"], "1.2.5.post11")
        self.assertEqual(summary["requested_partition"], "2")
        self.assertEqual(summary["outcome"], "recovered")
        self.assertEqual(summary["reason"], "retry_succeeded")
        self.assertEqual(summary["phase"], "zones")
        self.assertEqual(summary["zones_reads"], 2)
        self.assertEqual(summary["selection_attempts"], 1)
        self.assertEqual(summary["login_attempts"], 1)
        self.assertEqual(summary["zones_reconnects"], 0)
        self.assertIn("missing_zones", summary["issues"])
        self.assertEqual(summary["total_status_reads"], 1)
        self.assertEqual(summary["total_recovered"], 1)
        self.assertEqual(summary["total_failed"], 0)
        diagnostics = [json.loads(call.args[1]) for call in warning.call_args_list
                       if call.args[0] == "SIGMA_DIAG %s"]
        self.assertEqual(len(diagnostics), 1)
        self.assertEqual(diagnostics[0]["operation_id"], summary["operation_id"])
        self.assertEqual(diagnostics[0]["build"], summary["build"])
        self.assertEqual(summary["last_response"]["final_page"], "zones.html")
        self.assertEqual(summary["last_response"]["returned_partition"], "2")
        self.assertFalse(summary["last_response"]["login_token_present"])
        self.assertEqual(self.panel.commands, [])

    def test_status_summary_persistent_empty_zones_reports_six_reads(self):
        original = self.panel.get
        def get(url, **kwargs):
            if url.endswith("/zones.html"):
                return Response(html("2", kind="panel"), "zones.html")
            return original(url, **kwargs)
        self.panel.get = get
        with patch(PKG + ".partition_client._LOGGER.warning") as warning:
            with self.assertRaises(client_module.MissingZonesError):
                self.client.safe_get_partition_status("2")
        summaries = self.status_summaries(warning)
        self.assertEqual(len(summaries), 1)
        summary = summaries[0]
        self.assertEqual(summary["outcome"], "failed")
        self.assertEqual(summary["reason"], "missing_zones")
        self.assertEqual(summary["phase"], "zones")
        self.assertEqual(summary["zones_reads"], 6)
        self.assertEqual(summary["selection_attempts"], 2)
        self.assertEqual(summary["login_attempts"], 2)
        self.assertEqual(summary["zones_reconnects"], 1)
        self.assertEqual(summary["total_failed"], 1)
        self.assertEqual(summary["total_recovered"], 0)
        self.assertEqual(summary["last_response"]["http_status"], 200)
        self.assertEqual(summary["last_response"]["returned_partition"], "2")
        self.assertEqual(self.panel.commands, [])

    def test_status_summary_recovery_after_new_login_counts_one_operation(self):
        original = self.panel.get
        def get(url, **kwargs):
            if url.endswith("/zones.html") and self.panel.logins == 1:
                return Response(html("2", kind="panel"), "zones.html")
            return original(url, **kwargs)
        self.panel.get = get
        with patch(PKG + ".partition_client._LOGGER.warning") as warning:
            self.assertTrue(self.client.safe_get_partition_status("2")["zones"])
        summaries = self.status_summaries(warning)
        self.assertEqual(len(summaries), 1)
        summary = summaries[0]
        self.assertEqual(summary["outcome"], "recovered")
        self.assertEqual(summary["reason"], "retry_succeeded")
        self.assertEqual(summary["zones_reads"], 4)
        self.assertEqual(summary["selection_attempts"], 2)
        self.assertEqual(summary["login_attempts"], 2)
        self.assertEqual(summary["zones_reconnects"], 1)
        self.assertEqual(summary["total_status_reads"], 1)
        self.assertEqual(summary["total_recovered"], 1)
        self.assertEqual(summary["total_failed"], 0)
        self.assertEqual(self.panel.commands, [])

    def test_status_summary_panel_login_form_classified_as_session_expired(self):
        original = self.panel.get
        def get(url, **kwargs):
            if url.endswith("/panel.html"):
                return Response('<input name="gen_input" value="PRIVATE_TOKEN">', "login.html")
            return original(url, **kwargs)
        self.panel.get = get
        with patch(PKG + ".partition_client._LOGGER.warning") as warning:
            with self.assertRaises(client_module.SelectionSessionExpiredError):
                self.client.safe_get_partition_status("2")
        summaries = self.status_summaries(warning)
        self.assertEqual(len(summaries), 1)
        summary = summaries[0]
        self.assertEqual(summary["outcome"], "failed")
        self.assertEqual(summary["reason"], "session_expired")
        self.assertEqual(summary["phase"], "panel")
        self.assertEqual(summary["selection_attempts"], 2)
        self.assertEqual(summary["login_attempts"], 2)
        self.assertEqual(summary["zones_reads"], 0)
        self.assertEqual(summary["zones_reconnects"], 0)
        self.assertEqual(summary["last_response"]["final_page"], "login.html")
        self.assertTrue(summary["last_response"]["login_token_present"])
        self.assertIsNone(summary["last_response"]["returned_partition"])
        self.assertEqual(self.panel.commands, [])

    def test_status_summary_timeout_discards_previous_panel_response(self):
        original = self.panel.get
        panel_reads = 0
        def get(url, **kwargs):
            nonlocal panel_reads
            if url.endswith("/panel.html"):
                panel_reads += 1
                if panel_reads == 1:
                    return Response('<input name="gen_input" value="PRIVATE_TOKEN">', "login.html")
                raise requests.Timeout("PRIVATE_HOST PRIVATE_PIN PRIVATE_TIMEOUT")
            return original(url, **kwargs)
        self.panel.get = get
        with patch(PKG + ".partition_client._LOGGER.warning") as warning:
            with self.assertRaises(requests.Timeout):
                self.client.safe_get_partition_status("2")
        summary = self.status_summaries(warning)[0]
        self.assertEqual(summary["outcome"], "failed")
        self.assertEqual(summary["reason"], "timeout")
        self.assertEqual(summary["phase"], "panel")
        self.assertEqual(summary["last_response"], {})
        # Preserve post7's one outer request-error recovery after selection retries.
        self.assertEqual(summary["selection_attempts"], 4)
        self.assertEqual(summary["login_attempts"], 4)
        self.assertEqual(summary["zones_reconnects"], 1)
        self.assertEqual(summary["zones_reads"], 0)
        self.assertIn("session_expired", summary["issues"])
        self.assertIn("timeout", summary["issues"])
        self.assertNotIn("PRIVATE", json.dumps(summary))

    def test_status_summary_wrong_panel_then_expired_explicit_selection(self):
        original_get = self.panel.get
        original_post = self.panel.post
        def get(url, **kwargs):
            if url.endswith("/panel.html"):
                return Response(html("1", kind="panel"), "panel.html")
            return original_get(url, **kwargs)
        def post(url, data, **kwargs):
            if url.endswith("/part.cgi"):
                return Response('<input name="gen_input" value="PRIVATE_TOKEN">', "part.cgi")
            return original_post(url, data, **kwargs)
        self.panel.get, self.panel.post = get, post
        with patch(PKG + ".partition_client._LOGGER.warning") as warning:
            with self.assertRaises(client_module.SelectionSessionExpiredError):
                self.client.safe_get_partition_status("2")
        summaries = self.status_summaries(warning)
        self.assertEqual(len(summaries), 1)
        summary = summaries[0]
        self.assertEqual(summary["reason"], "session_expired")
        self.assertEqual(summary["phase"], "partition_selection")
        self.assertEqual(summary["selection_attempts"], 2)
        self.assertEqual(summary["login_attempts"], 2)
        self.assertEqual(summary["zones_reads"], 0)
        self.assertEqual(summary["zones_reconnects"], 0)
        self.assertEqual(summary["last_response"]["final_page"], "part.cgi")
        self.assertTrue(summary["last_response"]["login_token_present"])
        self.assertIsNone(summary["last_response"]["returned_partition"])
        self.assertEqual(self.panel.commands, [])

    def test_status_summary_never_contains_raw_response_or_exception_secrets(self):
        original = self.panel.get
        def get(url, **kwargs):
            if url.endswith("/zones.html"):
                response = Response(html("2", state="PRIVATE_UNKNOWN_STATUS") +
                                    '<title>PRIVATE_TITLE</title><input name="PRIVATE_NAME" value="PRIVATE_PIN">',
                                    "zones.html")
                response.url = "http://PRIVATE_USER:PRIVATE_PASS@PRIVATE_HOST/PRIVATE_PATH?token=PRIVATE_QUERY"
                response.history = [Response("PRIVATE_HTML", "login.html")]
                return response
            return original(url, **kwargs)
        self.panel.get = get
        with patch(PKG + ".partition_client._LOGGER.warning") as warning:
            with self.assertRaisesRegex(ValueError, "PRIVATE_UNKNOWN_STATUS"):
                self.client.safe_get_partition_status("2")
        summary = self.status_summaries(warning)[0]
        self.assertEqual(summary["reason"], "invalid_response")
        self.assertEqual(summary["phase"], "zones")
        self.assertEqual(summary["last_response"]["final_page"], "other")
        self.assertEqual(summary["last_response"]["redirects"], 1)
        self.assertNotIn("PRIVATE", json.dumps(summary))
        self.assertEqual(set(summary["last_response"]),
                         {"final_page", "http_status", "redirects", "redirect_pages",
                          "response_chars", "returned_partition", "login_token_present",
                          "auth_form_kind", "form_actions"})
        self.assertEqual(self.panel.commands, [])

    def test_status_summary_first_selection_failure_then_success_is_recovered(self):
        self.panel.reject_pin_once = True
        with patch(PKG + ".partition_client._LOGGER.warning") as warning:
            self.assertTrue(self.client.safe_get_partition_status("2")["zones"])
        summaries = self.status_summaries(warning)
        self.assertEqual(len(summaries), 1)
        summary = summaries[0]
        self.assertEqual(summary["outcome"], "recovered")
        self.assertEqual(summary["reason"], "retry_succeeded")
        self.assertEqual(summary["phase"], "zones")
        self.assertEqual(summary["selection_attempts"], 2)
        self.assertEqual(summary["login_attempts"], 2)
        self.assertEqual(summary["zones_reads"], 1)
        self.assertEqual(summary["zones_reconnects"], 0)
        self.assertEqual(summary["total_recovered"], 1)
        self.assertEqual(summary["total_failed"], 0)
        self.assertEqual(self.panel.commands, [])

    def test_status_summary_rate_limits_outcomes_separately_and_counts_all_polls(self):
        original = self.panel.get
        mode = "healthy"
        reads = 0
        clock = 100.0
        def get(url, **kwargs):
            nonlocal reads
            if url.endswith("/zones.html"):
                reads += 1
                if mode == "failed" or (mode == "recovered" and reads == 1):
                    return Response(html(self.panel.part, kind="panel"), "zones.html")
            return original(url, **kwargs)
        self.panel.get = get
        def poll(part, new_mode):
            nonlocal mode, reads
            mode, reads = new_mode, 0
            if mode == "failed":
                with self.assertRaises(client_module.MissingZonesError):
                    self.client.safe_get_partition_status(part)
            else:
                self.assertTrue(self.client.safe_get_partition_status(part)["zones"])
        with patch(PKG + ".partition_client._LOGGER.warning") as warning, \
                patch(PKG + ".partition_client.time.monotonic", side_effect=lambda: clock):
            poll("2", "healthy")  # Counted, but healthy operations remain silent.
            poll("2", "failed")
            clock = 101.0
            poll("2", "failed")  # Same partition/outcome suppressed.
            clock = 102.0
            poll("2", "recovered")  # A different outcome emits immediately.
            clock = 103.0
            poll("2", "recovered")  # This outcome is now suppressed too.
            clock = 104.0
            poll("1", "failed")  # Another partition has independent counters/rate limit.
            clock = 400.0
            poll("2", "failed")  # Exactly 300 seconds: publish cumulative counters.
            clock = 402.0
            poll("2", "recovered")
            clock = 700.0
            poll("2", "failed")  # Suppression count resets after the prior emission.
        summaries = self.status_summaries(warning)
        self.assertEqual(len(summaries), 6)
        self.assertEqual([(s["requested_partition"], s["outcome"]) for s in summaries],
                         [("2", "failed"), ("2", "recovered"), ("1", "failed"),
                          ("2", "failed"), ("2", "recovered"), ("2", "failed")])
        self.assertEqual([(s["total_status_reads"], s["total_recovered"], s["total_failed"],
                           s["suppressed_summaries"]) for s in summaries],
                         [(2, 0, 1, 0), (4, 1, 2, 0), (1, 0, 1, 0),
                          (6, 2, 3, 1), (7, 3, 3, 1), (8, 3, 4, 0)])
        self.assertEqual(len({s["operation_id"] for s in summaries}), len(summaries))
        self.assertEqual(self.panel.commands, [])

    def test_status_summary_reports_later_clean_recovery_only_for_failed_partition(self):
        original = self.panel.get
        fail_partition_two = True
        def get(url, **kwargs):
            if url.endswith("/zones.html") and self.panel.part == "2" and fail_partition_two:
                return Response(html("2", kind="panel"), "zones.html")
            return original(url, **kwargs)
        self.panel.get = get
        with patch(PKG + ".partition_client._LOGGER.warning") as warning:
            with self.assertRaises(client_module.MissingZonesError):
                self.client.safe_get_partition_status("2")
            self.assertTrue(self.client.safe_get_partition_status("1")["zones"])
            self.assertEqual(len(self.status_summaries(warning)), 1)
            fail_partition_two = False
            self.assertTrue(self.client.safe_get_partition_status("2")["zones"])
            self.assertTrue(self.client.safe_get_partition_status("2")["zones"])
        summaries = self.status_summaries(warning)
        self.assertEqual(len(summaries), 2)
        recovered = summaries[1]
        self.assertEqual(recovered["requested_partition"], "2")
        self.assertEqual(recovered["outcome"], "recovered")
        self.assertEqual(recovered["reason"], "subsequent_read_succeeded")
        self.assertIn("previous_read_failed", recovered["issues"])
        self.assertEqual(recovered["selection_attempts"], 1)
        self.assertEqual(recovered["zones_reads"], 1)
        self.assertEqual(recovered["zones_reconnects"], 0)
        self.assertEqual(recovered["total_status_reads"], 2)
        self.assertEqual(recovered["total_recovered"], 1)
        self.assertEqual(recovered["total_failed"], 1)
        self.assertNotEqual(recovered["operation_id"], summaries[0]["operation_id"])
        self.assertEqual(self.panel.commands, [])



if __name__ == "__main__":
    unittest.main(verbosity=2)
