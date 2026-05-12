# Kiến trúc Hệ thống Local Executor Client

## 1. Tổng quan (Overview)
Dự án là một Python-based Executor Client, thành phần then chốt trong hệ thống giao dịch Marcus. Nhiệm vụ chính là nhận tín hiệu từ Cloud Backend và thực thi lệnh ngay lập tức trên sàn giao dịch của người dùng để giảm thiểu độ trễ và tăng tính bảo mật (API Key được lưu trữ cục bộ, không rời khỏi máy người dùng).

## 2. Bản đồ Module (Module Map)

### [CLI]
- **File**: `src/local_executor/cli.py`
- **Responsibility**: Khởi tạo ứng dụng, nạp cấu hình từ biến môi trường và thiết lập logging.
- **Depends on**: `ExecutorConfig`, `LocalExecutorEngine`, `env_loader`.
- **Called by**: `local_executor.py` (root wrapper), `project.scripts`.
- **Key decision**: Tách biệt logic nạp môi trường (`env_loader`) khỏi logic khởi chạy engine.

### [Engine]
- **File**: `src/local_executor/engine.py`
- **Responsibility**: Điều phối chính luồng nhận tín hiệu và đẩy các bản tin giám sát (audit-push).
- **Depends on**: `ResilientWebSocketClient`, `CcxtSignalExecutor`, `ExecutorConfig`.
- **Called by**: `cli.py`.
- **Key decision**: Sử dụng `asyncio.wait` với `FIRST_COMPLETED` để quản lý đồng thời kết nối WebSocket và luồng đồng bộ số dư (`_balance_sync_loop`).

### [Executor]
- **File**: `src/local_executor/execution.py`
- **Responsibility**: Kiểm tra tính hợp lệ (validate) của tín hiệu và thực thi lệnh giao dịch qua thư viện CCXT.
- **Depends on**: `ccxt`, `ExecutorConfig`.
- **Called by**: `LocalExecutorEngine`.
- **Key decision**: Hỗ trợ chế độ `dry-run` để kiểm tra logic mà không thực hiện giao dịch thật.

### [WebSocket Client]
- **File**: `src/local_executor/ws_client.py`
- **Responsibility**: Duy trì kết nối WebSocket bền bỉ, xử lý xác thực HMAC và phân loại tin nhắn từ backend.
- **Depends on**: `websockets`, `hmac`, `hashlib`.
- **Called by**: `LocalExecutorEngine`.
- **Key decision**: Handshake sử dụng HMAC-SHA256 để đảm bảo tính xác thực của client.

### [Local Store]
- **File**: `src/local_executor/local_store.py`
- **Responsibility**: Lưu trữ bền bỉ trạng thái lệnh và các sự kiện thực thi vào cơ sở dữ liệu SQLite cục bộ.
- **Depends on**: `sqlite3`.
- **Called by**: `ExecutionStateEngine`, `ExecutionRecoveryManager`.
- **Key decision**: Áp dụng Outbox pattern cho việc gửi ACK để đảm bảo dữ liệu không bị mất khi mạng lỗi.

## 3. Luồng dữ liệu (Data Flow)

Hệ thống vận hành theo mô hình hướng sự kiện (event-driven):

1. **Nhận tín hiệu**: 
   - Backend Cloud -> `ResilientWebSocketClient` (Signal Frame) -> `LocalExecutorEngine`.
2. **Thực thi lệnh**:
   - `LocalExecutorEngine` -> `CcxtSignalExecutor` -> Sàn giao dịch (Exchange via CCXT).
3. **Phản hồi & Giám sát**:
   - Sàn giao dịch -> `CcxtSignalExecutor` (Result) -> `ExecutionEvent`.
   - `ExecutionEvent` -> `LocalExecutionStore` (Save to SQLite) -> `ExecutionACK` -> Backend Cloud.
4. **Đồng bộ trạng thái**:
   - `_balance_sync_loop` định kỳ lấy số dư từ sàn và gửi 'audit-push' về backend để cập nhật dashboard.

## 4. Phụ thuộc bên ngoài (External Dependencies)

- **CCXT**: Thư viện tiêu chuẩn để tương tác với hàng trăm sàn giao dịch Crypto khác nhau.
- **websockets**: Dùng để duy trì kết nối real-time hai chiều với backend.
- **sqlite3**: Dùng cho lưu trữ trạng thái cục bộ, hỗ trợ phục hồi sau sự cố.

## 5. Các khu vực rủi ro (Risk Areas)

- **Đồng bộ hóa trạng thái**: Việc duy trì sự đồng nhất giữa Local Store, Backend và Exchange trong điều kiện mạng không ổn định là thách thức lớn nhất (đang được xử lý bởi `RecoveryManager`).
- **Độ trễ thực thi**: Hiệu năng phụ thuộc vào tốc độ phản hồi của thư viện CCXT và mạng từ máy người dùng đến API của sàn.
- **Bảo mật API Key**: API Key được lưu tại file `.env` cục bộ. Người dùng cần được khuyến cáo bảo vệ file này tuyệt đối.
- **Xử lý Frame lỗi**: Hiện tại chỉ mới dừng lại ở việc tăng counter lỗi trong `ws_client.py`, cần bổ sung thêm cơ chế cảnh báo chủ động.

---
*Tài liệu này được tạo tự động sau quá trình khám phá codebase.*
