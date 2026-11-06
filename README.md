# CUKTECH BLE Local

在 Windows 上通过本机蓝牙监控和控制酷态科 10 Ultra。关闭浏览器后，BLE 后端仍在终端里运行，会继续采集数据并在掉线后尝试重连。

## 启动

开启 Windows 蓝牙，让充电器处于适配器的覆盖范围内。需要 Python 3.10 或更高版本。打开 PowerShell，进入项目目录后运行：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1
```

首次启动会联网安装 Python 依赖到项目内的 `.venv`。后续启动会复用已安装的依赖。启动后打开 [配置页](http://127.0.0.1:8199/config.html)，填入设备的 MAC、Token 和 BLE Key，点击「保存配置并重启」。已有凭据时也可以在服务停止后直接编辑 `data/config.yaml` 的 `ble` 段。使用配置页的「小米云自动获取」时，扫码导入阶段需要联网。

配置完成后打开 [本机控制台](http://127.0.0.1:8199/)。实时监控、端口控制和历史查询走本地 HTTP 与蓝牙。保留启动终端即可持续运行，按 `Ctrl+C` 停止两个进程。

如果 Web UI 端口 `8199` 已被占用，可在启动时指定其他端口，例如 `-WebPort 8200`，并用对应的地址打开页面。

## 后台运行

也可以直接双击项目根目录的 `start-background.bat`（后台启动）和 `stop-service.bat`（停止服务）：启动成功后窗口显示约 1 秒自动关闭，停止成功后窗口直接关闭；出错时窗口停住并显示错误，按任意键关闭。

不想保留终端窗口时，加 `-Background` 启动，服务会在当前用户会话里无窗口运行，命令在服务就绪后返回：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\start.ps1 -Background
```

停止后台服务：

```powershell
powershell -NoProfile -ExecutionPolicy Bypass -File .\stop.ps1
```

停止时后端会先断开蓝牙、收尾充电会话并关闭数据库，20 秒内未退出才会强制结束。后台运行时后端日志在 `data/server.log`，进程输出在 `data/console.log`。服务随当前 Windows 用户会话运行，注销后会停止。

## 项目结构

| 路径 | 用途 |
| --- | --- |
| `webui/` | 桌面、移动和配置页面及其静态资源 |
| `web_gateway.py` | 提供 Web UI，将同源 `/api/*` 请求和 SSE 转发给 BLE 后端 |
| `backend/` | Python BLE 后端，管理设备连接、HTTP API、SQLite 历史和设备配置 |
| `run_local.py` | 启动两个进程，在 Windows 上收到后端的重启退出码后重新拉起后端 |
| `start.ps1` / `stop.ps1` | 前台或后台（`-Background`）启动服务；停止后台服务 |
| `start-background.bat` / `stop-service.bat` | 双击后台启动 / 停止服务 |
| `data/` | 首次运行生成，存放配置和历史数据库 |

两个进程均监听 `127.0.0.1`。浏览器访问 Web UI 的端口，网关使用固定的内部端口 `18199` 访问 BLE 后端。保存配置只重启 BLE 后端，页面仍由网关提供。

代码基于 `cuktech-ble-server` 的 `481d53b`（`v2.0.0-Beta`）。BLE 协议、设备接口和页面来自上游，项目保留原许可证。 [上游源码](https://github.com/kairui1108/cuktech-ble-server/tree/481d53bfb7ccb65e576bf04a4b91f2b5a441bc8a) · [许可证](LICENSE)
