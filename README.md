# TikTok Live Monitor

基于 [TikTokLive](https://github.com/isaackogan/TikTokLive) 库的网页工具:输入一个 TikTok 直播间链接,实时展示该直播间里库能读取到的所有信息。

## 功能

- 输入直播间 URL(`https://www.tiktok.com/@用户名/live`)或直接输入 `@用户名` 即可连接
- **直播画面**:页面内直接播放直播视频(FLV 流,后端代理转发,mpegts.js 播放),支持切换清晰度,默认静音自动播放
- **房间信息**:标题、主播、房间 ID、开播时间,可查看完整 `room_info` JSON
- **实时统计**:当前在线观众、累计点赞、评论/礼物/钻石/进场/关注/分享计数
- **评论面板**:实时弹幕(含表情评论)
- **礼物面板**:礼物名称、图标、连击状态、钻石价值
- **互动面板**:进场、点赞、关注、分享、订阅
- **全部事件流**:库里 200+ 种事件全部捕获,逐条展示,点击展开原始 JSON,可过滤、可暂停
- **事件类型统计**:每种事件出现次数,点击可过滤事件流

## 安装

```bash
python3 -m venv venv
./venv/bin/pip install -r requirements.txt
```

## 运行

```bash
./start.sh
# 或自定义端口
PORT=9000 ./start.sh
```

然后浏览器打开 http://127.0.0.1:8000

## 技术结构

- `server.py` — FastAPI 后端。每个浏览器 WebSocket 连接对应一个 `TikTokLiveClient`,
  对库里所有事件类型注册监听器,把事件序列化成 JSON 推送给前端;
  另提供 `/proxy/flv` 把 TikTok CDN 的 FLV 直播流转发给浏览器(解决跨域,只允许 TikTok CDN 域名)。
- `static/index.html` — 单文件前端面板(无构建依赖)。

## 注意

- 只能连接**正在直播**的房间,主播未开播会提示
- 该库通过第三方签名服务器连接 TikTok,偶尔可能遇到限流,稍后重试即可
