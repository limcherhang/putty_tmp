def get_EPATaiwan_trans(language: str):
    if language.lower() == "tw":
        return "台灣環境部"
    elif language.lower() == "de":
        return "Bundesamt für Umwelt BAFU"
    elif language.lower() == "jp":
        return "気象庁"
    elif language.lower() == "it":
        return "Bureau di Meteo"
    elif language.lower() == "th":
        return "กรมอุตุนิยมวิทยา"
    elif language.lower() == "zh":
        return "环境部"

sheet1 = "Source"
sheet2 = "Table"

 # 創造一個變數作為數字和字母的對照表
    # {1: 'A', 2: 'B', 3: 'C', 4: 'D', 5: 'E', 6: 'F', 7: 'G', 8: 'H', 9: 'I', 10: 'J', 11: 'K', 12: 'L', 13: 'M', 14: 'N', 15: 'O', 16: 'P', 17: 'Q', 18: 'R', 19: 'S', 20: 'T', 21: 'U', 22: 'V', 23: 'W', 24: 'X', 25: 'Y', 26: 'Z', 27: 'AA', 28: 'AB', 29: 'AC', 30: 'AD', 31: 'AE', 32: 'AF', 33: 'AG', 34: 'AH', 35: 'AI', 36: 'AJ', 37: 'AK', 38: 'AL', 39: 'AM', 40: 'AN', 41: 'AO', 42: 'AP', 43: 'AQ', 44: 'AR', 45: 'AS', 46: 'AT', 47: 'AU', 48: 'AV', 49: 'AW', 50: 'AX', 51: 'AY', 52: 'AZ'}
chr_ord = {}
for i in range(65, 91):
    chr_ord[i-64] = chr(i)
    chr_ord[i-38] = "A"+chr(i)
    chr_ord[i-12] = "B" + chr(i)
