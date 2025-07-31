import paramiko
import csv

# Informasi login SFTP
hostname = "5.189.154.248"
port = 22
username = "heri"
password = "Passwd093"

# Nama file
original_file = "sales_data.csv"
remote_dir = "uploads/sales_data.csv"
new_file = "tiara_sales_data.csv"
upload_dir = "uploads/tiara_sales_data.csv"

# 1. Koneksi ke server dan unduh file
try:
    transport = paramiko.Transport((hostname, port))
    transport.connect(username=username, password=password)
    sftp = paramiko.SFTPClient.from_transport(transport)
    print("✅ Berhasil terhubung ke server")


    # Unduh file sales data
    sftp.get(remote_dir, original_file)
    print(f"✅ File '{original_file}' berhasil diunduh dari SFTP.")

    # 2. Baca dan proses isi file
    rows = []
    with open(original_file, mode='r') as file:
        reader = csv.DictReader(file)
        for row in reader:
            try:
                product = row['ProductName']
                quantity = int(row['QuantitySold'])
                price = float(row['Price'])
                total_amount = quantity * price
                row['total_amount'] = total_amount
                rows.append(row)
                print(f"Product: {product}, Quantity: {quantity}")
            except (ValueError, KeyError) as e:
                print(f"⚠️ Error pada {row} : {e}")
    
    if not rows:
        print(f"⚠️ Tidak ada data.")

    # 3. Simpan ke file baru dengan kolom tambahan
    fieldnames = list(rows[0].keys())
    with open(new_file, mode='w', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"✅ File baru '{new_file}' berhasil dibuat.")

    # 4. Upload file baru ke direktori 'uploads' di SFTP
    sftp.put(new_file, upload_dir)
    print(f"✅ File '{new_file}' berhasil diunggah ke direktori {upload_dir}.")

# Tutup koneksi
except paramiko.AuthenticationException:
    print("❌ Otentikasi gagal. Cek kembali kredensial Anda.")
except paramiko.SSHException as ssh_exception:
    print(f"❌ Tidak dapat tersambung ke SSH: {ssh_exception}")
except paramiko.SFTPError as sftp_error:
    print(f"❌ SFTP error: {sftp_error}")
except FileNotFoundError as fnf_error:
    print(f"❌ File tidak ditemukan: {fnf_error}")
except ConnectionError:
    print("❌ Tidak dapat terhubung ke server SFTP.")
except Exception as e:
    print(f"❌ Error: {e}")

finally:
    sftp.close()
    transport.close()
    print("✅ Koneksi SFTP berhasil ditutup.")