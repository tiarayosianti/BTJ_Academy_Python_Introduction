# Latihan 2.2: Penyaringan Data Dasar dan Menulis ke File Baru

# Saring produk dengan harga > 100.00
high_value_products = [item for item in product_list if item['Price'] > 100.00]

# Tulis ke file baru
new_path = 'D:/BTJ Academy/SQL_for_data/tiara_high_value_products.txt'
with open(new_path, "w") as file:
    # Header
    file.write("Product,Price\n")
    
    # Tulis baris produk
    for product in high_value_products:
        line = f"{product['Product']},{product['Price']:.2f}\n"
        file.write(line)

print("File 'tiara_high_value_products.txt' berhasil dibuat dengan data produk yang berharga lebih dari 100.00")
