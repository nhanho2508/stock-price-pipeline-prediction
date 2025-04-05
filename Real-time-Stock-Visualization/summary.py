import os
import pandas as pd

# Danh sách các ngân hàng và tương ứng với tên file
banks = ["AXIS", "HDFC", "ICICI", "KOTAK", "RBL"]
folder_path = "data/"  # Thay đổi thành thư mục chứa các file

# Danh sách để chứa DataFrame của từng ngân hàng
dataframes = []
# Đọc và xử lý từng file
for bank in banks:
    file_path = os.path.join(folder_path, f"{bank}BANK__EQ__NSE__NSE__MINUTE.csv")  # Giả sử file có tên là AXIS.csv, HDFC.csv, ...
    
    if os.path.exists(file_path):  
        df = pd.read_csv(file_path)

        # Kiểm tra nếu cột đầu tiên chứa chữ "timestamp" thì bỏ dòng đầu tiên (header)
        if df.iloc[0, 0].lower() == "timestamp":
            df = df.iloc[1:]  # Bỏ dòng đầu tiên

        # Đặt lại tên cột chính xác
        df.columns = ["timestamp", "open", "high", "low", "close", "volume"]

        # Thêm cột bank_name
        df.insert(1, "bank_name", bank)

        # Nếu không phải file ICICI, xóa timezone (+05:30)
        if bank != "ICICI":
            df["timestamp"] = df["timestamp"].str.replace(r"\+05:30", "", regex=True)

        # Chuyển đổi timestamp về kiểu datetime (không có timezone)
        df["timestamp"] = pd.to_datetime(df["timestamp"], format="%Y-%m-%d %H:%M:%S", errors="coerce")

        # Kiểm tra nếu có giá trị NaT (lỗi), in ra cảnh báo
        if df["timestamp"].isna().sum() > 0:
            print(f"Warning: Có {df['timestamp'].isna().sum()} giá trị timestamp lỗi trong file {file_path}")

        # Thêm vào danh sách DataFrame
        dataframes.append(df)
    else:
        print(f"File {file_path} không tồn tại!")

# Gộp tất cả DataFrame
if dataframes:
    final_df = pd.concat(dataframes, ignore_index=True)

    # Sắp xếp theo timestamp
    final_df = final_df.sort_values(by="timestamp")

    # Xuất ra file CSV mới
    final_df.to_csv("merged_banks_sorted.csv", index=False)
    print("Đã gộp và sắp xếp dữ liệu thành merged_banks_sorted.csv")
else:
    print("Không có dữ liệu để gộp!")