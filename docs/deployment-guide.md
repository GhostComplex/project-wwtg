# Deployment Guide — 周末去呀

## Prerequisites (Juanjuan 需要完成)

### 1. 创建 Azure Container Registry (ACR)
- Azure Portal → 搜索「容器注册表」→ 创建
- 资源组: `rg-weekend-buddy`
- 注册表名称: `wwtgacr` (或其他名称)
- SKU: Basic (~¥35/月)
- 创建后 → 左侧「访问密钥」→ 启用「管理员用户」→ 记录：
  - 登录服务器 (如 `wwtgacr.azurecr.io`)
  - 用户名
  - 密码

### 2. 创建 Azure Service Principal
- 需要有 Azure CLI 或 Cloud Shell
- 运行:
  ```bash
  az ad sp create-for-rbac --name "wwtg-github" --role contributor \
    --scopes /subscriptions/{subscription-id}/resourceGroups/rg-weekend-buddy \
    --sdk-auth
  ```
- 保存输出的 JSON

### 3. 配置 GitHub Secrets
- 进入 GitHub repo → Settings → Secrets and variables → Actions
- 添加以下 Secrets:
  - `ACR_LOGIN_SERVER` — ACR 登录服务器地址
  - `ACR_USERNAME` — ACR 管理员用户名
  - `ACR_PASSWORD` — ACR 管理员密码
  - `AZURE_CREDENTIALS` — Step 2 的 JSON 输出

### 4. Container App 配置 ACR 拉取
- Azure Portal → Container App `wwtg-api` → 左侧「容器」→ 编辑
- 镜像来源改为你的 ACR
- 或者在「机密」中添加 ACR 凭据

### 5. 触发部署
- 合并 PR 到 `main` 分支会自动触发 CD
- 或手动: Actions → cd → Run workflow

## 验证
- 访问 `https://wwtg-api.redisland-5b339c5d.eastasia.azurecontainerapps.io/health`
- 期望返回: `{"status": "ok", "db": "ok", "redis": "ok"}`
