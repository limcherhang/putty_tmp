import fitz  # PyMuPDF

def extract_images_from_pdf(pdf_path, output_folder):
    # 打开PDF文件
    pdf_document = fitz.open(pdf_path)
    
    # 遍历PDF中的每一页
    for page_number in range(len(pdf_document)):
        # 获取当前页面对象
        page = pdf_document[page_number]
        
        # 获取页面中的所有图片
        image_list = page.get_images(full=True)
        print(image_list)
        
        # 遍历页面中的每个图片
        for image_index, img in enumerate(image_list):
            # 获取图片的字典信息
            xref = img[0]
            base_image = pdf_document.extract_image(xref)
            image_bytes = base_image["image"]
            
            # 将图片保存到文件中
            image_path = f"{output_folder}/page_{page_number}_image_{image_index}.png"
            with open(image_path, "wb") as image_file:
                image_file.write(image_bytes)
    
    # 关闭PDF文件
    pdf_document.close()

# 调用函数来提取PDF中的图片
pdf_path = "test.pdf"
output_folder = "output_images"
extract_images_from_pdf(pdf_path, output_folder)