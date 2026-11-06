"""CUKTECH BLE Server - Configuration management.

Supports YAML config file and environment variables.
YAML file takes precedence over environment variables.
"""
import logging
import os
import re
import sys
from dataclasses import dataclass, field
from pathlib import Path

_LOGGER = logging.getLogger(__name__)

LOG_LEVELS = {
    "debug": logging.DEBUG,
    "info": logging.INFO,
    "warning": logging.WARNING,
    "error": logging.ERROR,
}

# primary 设备的固定标识（§3.10）。history/meta 的兼容全部锚定在它上面，
# 因此**不可配置**：老库里的行、老 Web UI 的请求都默认它。
PRIMARY_DEVICE_ID = "default"

# 设备 id 会出现在 URL 路径（`/api/device/{id}/...`）与历史库的设备列里
_DEVICE_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_-]*$")
_RESERVED_DEVICE_IDS = frozenset({PRIMARY_DEVICE_ID})


def _load_yaml_config(path=None):
    """Load config from YAML file if exists.

    `path` 显式指定时只读这一个文件（保存前试算用）：此时**不允许**回退到 cwd，
    否则"验的是别的文件"这种事故会静默发生。
    """
    if path is not None:
        config_path = Path(path)
        if not config_path.exists():
            return {}
    else:
        config_path = Path(os.environ.get("CUKTECH_CONFIG_PATH", str(Path(__file__).parent / "config.yaml")))
        if not config_path.exists():
            config_path = Path.cwd() / "config.yaml"
        if not config_path.exists():
            return {}
    try:
        import yaml
        with open(config_path, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except ImportError:
        return {}


@dataclass
class BLEConfig:
    mac: str = ""
    token: str = ""
    ble_key: str = ""
    scan_timeout: int = 10
    # 显示名（可选）：多设备时 Web UI 用它区分；为空则回落 "default"。
    name: str = ""

    def __post_init__(self):
        if not self.mac or self.mac == "XX:XX:XX:XX:XX:XX":
            _LOGGER.warning("CUKTECH_DEVICE_MAC 未配置，BLE 连接不可用。可通过 config.html 配置或设置环境变量")
        if not self.token:
            _LOGGER.warning("CUKTECH_DEVICE_TOKEN 未配置，BLE 连接不可用。可通过 config.html 配置或设置环境变量")


@dataclass
class DeviceConfig:
    """一台充电器的配置（§3.10）。

    primary 由老 `ble:` 节合成，`device_id` 固定 `default`；其余来自 `devices:` 节。
    两类在同一个类型里表达，因为**下游（registry/manager）看待它们没有区别**。
    """

    device_id: str = PRIMARY_DEVICE_ID
    name: str = ""
    mac: str = ""
    token: str = ""
    ble_key: str = ""
    scan_timeout: int = 10
    is_primary: bool = False

    @property
    def display_name(self) -> str:
        return self.name or self.device_id

    @classmethod
    def from_mapping(cls, item: dict, *, default_scan_timeout: int = 10) -> "DeviceConfig":
        """从 `devices:` 节的一项（或等价的 dict）构造。

        接受 `id` 与 `device_id` 两种键：配置文件/schema 里叫 `id`（短、YAML 友好），
        程序里叫 `device_id`（与 registry 键、history 列一致）。两种写法都收，避免
        调用方记两套名字。
        """
        device_id = str(item.get("id") or item.get("device_id") or "").strip()
        return cls(
            device_id=device_id,
            name=str(item.get("name") or "").strip(),
            mac=str(item.get("mac") or "").strip(),
            token=str(item.get("token") or "").strip(),
            ble_key=str(item.get("ble_key") or "").strip(),
            scan_timeout=int(item.get("scan_timeout", default_scan_timeout)),
            is_primary=False,
        )


@dataclass
class ServerConfig:
    host: str = "127.0.0.1"
    port: int = field(default_factory=lambda: 18199 if sys.platform == "win32" else 8199)
    command_timeout: float = 10.0
    settings_refresh_interval: float = 10.0
    log_level: str = "info"
    history_retention_days: int = 2
    history_db_path: str = "port_history.db"
    reconnect_base_delay: float = 1.0
    reconnect_max_delay: float = 300.0
    # 启动错峰（§3.4）：第 index 台设备启动前等 index × 该值。N=1 恒为 0 延迟，
    # 所以单设备时序与改造前完全一致；多设备时避免"10 台同时上电 → 认证风暴"。
    startup_stagger_sec: float = 2.0


@dataclass
class Config:
    ble: BLEConfig = field(default_factory=BLEConfig)
    server: ServerConfig = field(default_factory=ServerConfig)
    # 附加设备（`devices:` 节）。**不含 primary**：primary 永远由 `ble:` 节合成，
    # 所以 `devices` 为空列表 = 单设备部署，行为与改造前 100% 一致。
    devices: list = field(default_factory=list)

    # ── 设备视图（§3.10：primary 来自 ble: 节，其余来自 devices:） ──

    def primary_device(self) -> DeviceConfig:
        return DeviceConfig(
            device_id=PRIMARY_DEVICE_ID,
            name=self.ble.name or "",
            mac=self.ble.mac,
            token=self.ble.token,
            ble_key=self.ble.ble_key,
            scan_timeout=self.ble.scan_timeout,
            is_primary=True,
        )

    def device_specs(self) -> list:
        """registry 的唯一输入：primary 在前，附加设备按配置顺序在后。"""
        specs = [self.primary_device()]
        for dev in self.devices:
            specs.append(dev if isinstance(dev, DeviceConfig)
                         else DeviceConfig.from_mapping(dev, default_scan_timeout=self.ble.scan_timeout))
        return specs


def load_config(path=None) -> Config:
    """Load config from YAML file, then override with environment variables.

    `path` 让"保存配置前试算"走**与启动完全同一条路**（§3.10 不变式：保存 =
    启动可接受）。另写一份校验必然与这里漂移，而漂移的代价是"存得进去、起不来"
    ——重启被 load_config 拒绝，Web UI 也就跟着没了，用户只能手改 YAML 救回来。
    """
    return config_from_mapping(_load_yaml_config(path))


def config_from_mapping(ycfg: dict) -> Config:
    """把 YAML 映射解析成 Config（env 覆盖 + `validate_device_specs` 都在这里）。

    启动（`load_config`）与"保存前试算"共用同一条实现，不各写一份。
    """

    ble_cfg = ycfg.get("ble", {})
    server_cfg = ycfg.get("server", {})

    ble = BLEConfig(
        mac=os.environ.get("CUKTECH_DEVICE_MAC", ble_cfg.get("mac", "")),
        token=os.environ.get("CUKTECH_DEVICE_TOKEN", ble_cfg.get("token", "")),
        ble_key=os.environ.get("CUKTECH_DEVICE_BLE_KEY", ble_cfg.get("ble_key", "")),
        scan_timeout=ble_cfg.get("scan_timeout", 10),
        name=ble_cfg.get("name", ""),
    )

    try:
        history_retention = int(os.environ.get("CUKTECH_HISTORY_RETENTION_DAYS", server_cfg.get("history_retention_days", 2)))
    except (ValueError, TypeError) as e:
        raise ValueError(f"History retention days must be an integer: {e}")
    try:
        reconnect_base_delay = float(server_cfg.get("reconnect_base_delay", 1.0))
    except (ValueError, TypeError) as e:
        raise ValueError(f"Reconnect base delay must be a number: {e}")
    try:
        reconnect_max_delay = float(server_cfg.get("reconnect_max_delay", 300.0))
    except (ValueError, TypeError) as e:
        raise ValueError(f"Reconnect max delay must be a number: {e}")

    server = ServerConfig(
        host=os.environ.get("CUKTECH_SERVER_HOST", server_cfg.get("host", "127.0.0.1")),
        port=int(os.environ.get("CUKTECH_SERVER_PORT", server_cfg.get("port", 18199 if sys.platform == "win32" else 8199))),
        command_timeout=server_cfg.get("command_timeout", 10.0),
        settings_refresh_interval=server_cfg.get("settings_refresh_interval", 60.0),
        log_level=os.environ.get("CUKTECH_LOG_LEVEL", server_cfg.get("log_level", "info")),
        history_retention_days=history_retention,
        history_db_path=os.environ.get("CUKTECH_HISTORY_DB_PATH", server_cfg.get("history_db_path", "port_history.db")),
        reconnect_base_delay=reconnect_base_delay,
        reconnect_max_delay=reconnect_max_delay,
        startup_stagger_sec=float(server_cfg.get("startup_stagger_sec", 2.0)),
    )

    devices = _parse_devices(ycfg, ble)
    cfg = Config(ble=ble, server=server, devices=devices)
    validate_device_specs(cfg)
    return cfg


# ── 多设备配置：解析与启动校验（§3.10） ──

def _parse_devices(ycfg: dict, ble: BLEConfig) -> list:
    """把 `devices:` 节解析成 `DeviceConfig` 列表（不含 primary）。

    **字段清洗只有一份实现**（`DeviceConfig.from_mapping`）：这里以前自己抄了一遍
    键名（只认 `id`，而 from_mapping 认 `id`/`device_id`）、字段清洗与默认
    `scan_timeout`，两套解析器迟早会漂移——而漂移的后果是"配置页存得进、启动读
    不出"或者反过来。本函数只保留**它独有的价值**：带 `devices[i]` 下标的报错
    （YAML 里有几十行时，能直接跳到出错那一条）与列表形状检查。
    """
    raw = ycfg.get("devices")
    if raw in (None, ""):
        return []
    if raw == []:
        # 显式空节：等价单设备（无附加设备）。给配置工具留一个"确实没有附加设备"的表达。
        return []
    if not isinstance(raw, list):
        raise ValueError("配置错误：`devices` 必须是列表（每项一台附加充电器）")
    specs: list = []
    for idx, item in enumerate(raw):
        if not isinstance(item, dict):
            raise ValueError(f"配置错误：`devices[{idx}]` 必须是映射（id/mac/token）")
        spec = DeviceConfig.from_mapping(item, default_scan_timeout=ble.scan_timeout)
        # 必填校验：报错要能定位到具体哪一条，所以留在这里而不是 from_mapping
        if not spec.device_id:
            raise ValueError(f"配置错误：`devices[{idx}]` 缺少 `id`")
        if not spec.mac:
            raise ValueError(f"配置错误：设备 `{spec.device_id}` 缺少 `mac`")
        if not spec.token:
            raise ValueError(f"配置错误：设备 `{spec.device_id}` 缺少 `token`")
        specs.append(spec)
    return specs


def validate_device_specs(cfg: Config) -> None:
    """启动期设备校验（§3.10）。有问题一律 raise ValueError，绝不"能跑就先跑"。

    多设备串台的代价是**控制到错误的充电器**（安全事故），所以宁可拒绝启动。
    """
    specs = cfg.device_specs()
    extras = specs[1:]

    if extras and not (cfg.ble.mac and cfg.ble.token):
        raise ValueError(
            "配置错误：primary 设备必须在 `ble:` 节配置（mac/token）；"
            "`devices:` 只是附加设备")

    # ── id ──
    seen_ids = {PRIMARY_DEVICE_ID}
    for spec in extras:
        if not _DEVICE_ID_RE.match(spec.device_id):
            raise ValueError(
                f"配置错误：设备 id `{spec.device_id}` 非法（只允许小写字母/数字/_-，且以字母或数字开头）")
        if spec.device_id in _RESERVED_DEVICE_IDS:
            raise ValueError(
                f"配置错误：设备 id `{spec.device_id}` 与保留字冲突"
                f"（{', '.join(sorted(_RESERVED_DEVICE_IDS))}）")
        if spec.device_id in seen_ids:
            raise ValueError(f"配置错误：设备 id `{spec.device_id}` 重复")
        seen_ids.add(spec.device_id)

    # ── mac：全局唯一（跨节） ──
    seen_macs: dict = {}
    for spec in specs:
        mac = (spec.mac or "").strip().upper()
        if not mac:
            continue
        if mac in seen_macs:
            raise ValueError(
                f"配置错误：MAC {spec.mac} 同时出现在 "
                f"`{seen_macs[mac]}` 与 `{spec.device_id}`")
        seen_macs[mac] = spec.device_id

    # ── token：必须是合法 hex（BLEManager 构造即 bytes.fromhex） ──
    for spec in extras:
        token = (spec.token or "").strip()
        try:
            if len(token) % 2:
                raise ValueError
            bytes.fromhex(token)
        except ValueError:
            raise ValueError(
                f"配置错误：设备 `{spec.device_id}` 的 token 不是合法十六进制串") from None
