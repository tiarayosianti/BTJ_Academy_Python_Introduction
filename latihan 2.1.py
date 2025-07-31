# # Persiapan: Buat file bernama data.txt di folder kerja kalian dengan isi sebagai berikut:
data_product = [
    'Product,Quantity,Price',
    'Laptop,1,1200.00',
    'Mouse,2,25.50',
    'Keyboard,1,75.00',
    'Monitor,1,300.00',
    'Headphones,5,50.00'
]

file_path = 'D:/BTJ Academy/SQL_for_data/tiara_data.txt'
with open(file_path, 'w') as file:
    for line in data_product:
        file.write(line + '\n')

# Latihan 2.1: Membaca dan Memilah File Mirip CSV Sederhana
# Buka file data.txt untuk membaca
with open(file_path, 'r') as file:
    # Baca baris header-nya secara terpisah.
    header = file.readline()
    header = header.strip().split(',')

    lines = file.readlines()
    lines = [line.strip() for line in lines]

    product_list = []

    # Lakukan loop pada baris-baris selanjutnya. Untuk setiap baris, 
    # pisahkan berdasarkan koma (,) lalu tampilkan Produk dan Kuantitas-nya.
    for line in lines:
        line = line.strip().split(',')
        product = line[0]
        quantity = int(line[1])
        price = float(line[2])

        # Menampilkan produk dan kuantitas
        print(f"Produk: {product}, Kuantitas: {quantity}")

        # Simpan dalam bentuk dictionary
        product_dict = {
            'Product': product,
            'Quantity': quantity,
            'Price': price
        }
        product_list.append(product_dict)

print(product_list)