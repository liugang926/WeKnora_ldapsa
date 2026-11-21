# Nextcloud WebDAV 单向数据源

通过标准 WebDAV 将选定目录里的支持格式导入 WeKnora。Nextcloud 无需安装插件。管理员为连接器配置专用 Nextcloud 服务账号及其应用密码，手动和定时任务共用数据源保存的这组凭证。服务账号的读权限决定可同步的源文件；入库后的搜索、问答和读取由目标 WeKnora 知识库授权。

## 编译和使用

在 WeKnora 源码根目录运行以下命令。Go 编译叠加文件只在本次构建中向服务端入口添加注册代码。可以给原有构建命令保留所需的标签、版本信息及其他参数。

```sh
go build -overlay=extensions/nextcloudwebdav/integration/overlay.json -o WeKnora ./cmd/server
```

通过正常部署流程使用该二进制后，在目标知识库的「设置 → 数据源 → 添加数据源」中选择 **Nextcloud WebDAV**。点击已保存的数据源卡片可以编辑该源。也可以通过同一个 WeKnora 站点进入 `/api/v1/extensions/nextcloud-webdav/`。页面使用当前登录的工作区身份。具备相应管理和目标知识库编辑权限的用户可以配置数据源。配置包括 Nextcloud 实例地址（例如 `https://cloud.example.org`）、专用服务账号和该账号的应用密码。保存暂停的数据源后，选择目录、保存计划并启用。选择服务账号的文件根目录表示将整个可读取范围作为同步候选。

扩展注册的类型是 `nextcloud_webdav`。保存后可以在该知识库已有的数据源列表中查看同步状态和历史。凭证由普通数据源的凭证接口成组替换，读取数据源配置时仅返回配置状态。定时任务沿用此分支现有的普通连接器计划授权；保存计划的编辑者需持续拥有所需权限。

## 升级核验

从未使用编译叠加文件的构建中，官方服务端不会加载此扩展。升级 WeKnora 后，可以先运行扩展测试，再检查编译入口：

```sh
go test ./extensions/nextcloudwebdav/...
node --test extensions/nextcloudwebdav/adminui/manager.test.mjs
go list -overlay=extensions/nextcloudwebdav/integration/overlay.json -f '{{.GoFiles}}' ./cmd/server
```

入口清单应包含 `nextcloud_webdav_extension.go`。普通构建的同一清单没有这个文件。保留扩展目录和已授权的同步计划，再发布经过核验的新二进制。
