import paramiko
import csv

class SFTPProcessor:
    def __init__(self, name):
        # Informasi login SFTP
        self.name = name
        self.hostname = "5.189.154.248"
        self.port = 22
        self.username = "heri"
        self.password = "Passwd093"

        # Nama file
        self.original_file = "sales_data.csv"
        self.remote_dir = "uploads/sales_data.csv"
        self.new_file = f"{self.name}_sales_data.csv"
        self.upload_dir = f"uploads/{self.new_file}"

        self.rows = []

        # 1. Koneksi ke server
        try:
            self.transport = paramiko.Transport((self.hostname, self.port))
            self.transport.connect(username=self.username, password=self.password)
            self.sftp = paramiko.SFTPClient.from_transport(self.transport)
            print("Berhasil terhubung ke server")
        except paramiko.AuthenticationException:
            print("Otentikasi gagal. Cek kembali kredensial Anda.")
        except paramiko.SSHException as ssh_exception:
            print(f"Tidak dapat tersambung ke SSH: {ssh_exception}")
        except paramiko.SFTPError as sftp_error:
            print(f"SFTP error: {sftp_error}")
        except ConnectionError:
            print("Tidak dapat terhubung ke server SFTP.")
        except Exception as e:
            print(f"Error: {e}")

    # 2. Unduh file
    def download_file_dari_sftp(self):
        try:
            self.sftp.get(self.remote_dir, self.original_file)
            print(f"File '{self.original_file}' berhasil diunduh dari SFTP.")
        except Exception as e:
            print(f"Gagal mengunduh file: {e}")

    # 3. Baca dan proses isi file
    def transform_file(self):
        try:
            with open(self.original_file, mode='r') as file:
                reader = csv.DictReader(file)
                for row in reader:
                    try:
                        product = row['ProductName']
                        quantity = int(row['QuantitySold'])
                        price = float(row['Price'])
                        total_amount = quantity * price
                        row['total_amount'] = total_amount
                        self.rows.append(row)
                        print(f"Product: {product}, Quantity: {quantity}")
                    except (ValueError, KeyError) as e:
                        print(f"Error pada baris {row}: {e}")

            if not self.rows:
                print("Tidak ada data yang diproses.")
                return
            
            # 4. Simpan ke file baru dengan kolom tambahan
            fieldnames = list(self.rows[0].keys())
            with open(self.new_file, mode='w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(self.rows)
            print(f"File baru '{self.new_file}' berhasil dibuat.")
        except FileNotFoundError:
            print("File sumber tidak ditemukan.")
        except Exception as e:
            print(f"Gagal memproses file: {e}")

    # 5. Upload file baru ke direktori 'uploads' di SFTP
    def load_to_sftp(self):
        try:
            self.sftp.put(self.new_file, self.upload_dir)
            print(f"File '{self.new_file}' berhasil diunggah ke {self.upload_dir}.")
        except Exception as e:
            print(f"Gagal mengunggah file: {e}")

    # 6. Tutup koneksi
    def close_connection(self):
        try:
            self.sftp.close()
            self.transport.close()
            print("Koneksi SFTP berhasil ditutup.")
        except:
            print("Koneksi sudah tertutup atau belum terbuka.")

if __name__ == "__main__":
    processor = SFTPProcessor("tiara2")
    processor.download_file_dari_sftp()
    processor.transform_file()
    processor.load_to_sftp()
    processor.close_connection()
