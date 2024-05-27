from PIL import Image

def de2hex(d):
    return int(d, )

# 打開圖像
image = Image.open("image.png")

# 獲取指定像素的RGB值
rgb_value = image.getpixel((100, 100))[:3]  # (x, y) 是像素的坐標

hex_value = ''.join(hex(x)[2:].zfill(2) for x in rgb_value)

# 將結果轉換為RRGGBB格式（帶有 '#' 前綴）
rrggbb_format = '#' + hex_value.upper()

print(rrggbb_format)  # 輸出結果：#ECE4DD