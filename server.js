// Relay WebSocket đơn giản: mỗi client (trình duyệt) kết nối vào server NÀY, server sẽ tự
// mở 1 kết nối WebSocket THẬT tới MEXC Futures (wss://contract.mexc.com/edge) rồi chuyển tiếp
// (forward) dữ liệu 2 chiều y nguyên — client không cần biết gì về việc này.
//
// Lý do cần cái này: MEXC chặn các kết nối WebSocket đến thẳng từ trình duyệt của bạn (không
// phải do CORS, mà do kiểm tra Origin/khu vực ở tầng WS handshake) — dùng VPN vẫn không qua được
// nghĩa là họ chặn theo Origin chứ không phải theo IP/khu vực. Vì kết nối THẬT bây giờ xuất phát
// từ chính server Node.js này (không phải trình duyệt), sẽ không bị áp cùng kiểu chặn đó.
//
// CÁCH DÙNG:
//   1) npm install
//   2) Deploy lên Railway/Render/Fly.io (free tier đều chạy được, vì đây là server Node bình
//      thường, giữ kết nối lâu dài được — khác Vercel serverless không giữ được WS).
//   3) Sau khi deploy xong sẽ có 1 domain dạng: your-app.up.railway.app
//   4) Trong file index.html (chart), đổi:
//        const FUTURES_WS_URL = 'wss://contract.mexc.com/edge';
//      thành:
//        const FUTURES_WS_URL = 'wss://your-app.up.railway.app';
//      (nhớ thêm 's' -> wss vì Railway/Render tự cấp HTTPS/WSS cho domain của bạn)

const { WebSocketServer, WebSocket } = require('ws');
const http = require('http');

const PORT = process.env.PORT || 8080;
const UPSTREAM = 'wss://contract.mexc.com/edge';

const server = http.createServer((req, res) => {
  // Endpoint kiểm tra sống (Railway/Render hay ping cái này để biết server còn chạy không)
  res.writeHead(200, { 'Content-Type': 'text/plain' });
  res.end('mexc-ws-relay OK\n');
});

const wss = new WebSocketServer({ server });

wss.on('connection', (client) => {
  console.log('[relay] client mới kết nối, đang mở kết nối thật tới MEXC...');

  const upstream = new WebSocket(UPSTREAM);
  // Hàng đợi tạm cho các message client gửi TRƯỚC khi upstream kịp mở (readyState chưa OPEN)
  const pending = [];

  upstream.on('open', () => {
    console.log('[relay] đã kết nối MEXC thật, bắt đầu forward.');
    for (const msg of pending) upstream.send(msg);
    pending.length = 0;
  });

  // MEXC -> relay -> client
  upstream.on('message', (data) => {
    if (client.readyState === WebSocket.OPEN) client.send(data);
  });
  // Log rõ MÃ LÝ DO đóng kết nối (close code/reason) — để biết chính xác AI đóng trước: MEXC chủ
  // động đóng (vd rate-limit, không đúng định dạng ping...), hay do hạ tầng Railway ngắt vì lý do
  // khác (idle timeout, giới hạn tài nguyên...). Không có log này thì không thể chẩn đoán tiếp được.
  upstream.on('close', (code, reason) => {
    console.log(`[relay] MEXC đóng kết nối — code=${code} reason=${reason ? reason.toString() : '(không có)'}`);
    try { client.close(); } catch (e) {}
  });
  upstream.on('error', (e) => { console.error('[relay] lỗi kết nối upstream MEXC:', e.message); try { client.close(); } catch (e2) {} });

  // client -> relay -> MEXC
  client.on('message', (data) => {
    if (upstream.readyState === WebSocket.OPEN) upstream.send(data);
    else pending.push(data);
  });
  client.on('close', (code, reason) => {
    console.log(`[relay] client (trình duyệt) đóng kết nối — code=${code} reason=${reason ? reason.toString() : '(không có)'}`);
    try { upstream.close(); } catch (e) {}
  });
  client.on('error', () => { try { upstream.close(); } catch (e) {} });
});

server.listen(PORT, () => console.log('[relay] đang chạy ở cổng ' + PORT));
