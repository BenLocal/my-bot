# 开发环境 / 推送 / 发布

> 凭据存放约定、镜像源与代理、k3s 部署操作；动环境或部署前先看本文。

## 开发环境信息
> ⚠️ 执行器账号、模型 API key、API token 等**不入库**：本地放未入库的 `dev-env.local.md`（已在 `.gitignore`），集群里放 K8s Secret。新增凭据一律照此处理。

- 本地联调依赖：docker（Redis）、Dapr CLI（尚未安装，M0 时装并记录版本）。
- 核验执行器 RPC 协议时，以 Worker 镜像中的 pi / codex 版本为准，不以开发机上的版本为准。

## 网络
- 构建镜像时 npm 走 npmmirror，Go 模块可用 `GOPROXY=https://goproxy.cn,direct`。
- 本机代理对大文件不稳定；git push 卡住时绕过代理：`HTTPS_PROXY= HTTP_PROXY= git push`。

## 部署（k3s）
- k3s 尚未安装。安装需要 root：写成 `/tmp/*.sh` 脚本交给用户在外部终端执行，执行后再验证（`kubectl get nodes`）。
- 顺序：k3s → Dapr Helm（`dapr-system`）→ `kubectl apply -k deploy/k8s/overlays/k3s`。
- Codex Worker 首次部署后需进 Pod 执行设备码登录（spec §7.3），由用户操作。
