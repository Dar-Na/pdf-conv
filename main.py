import PyPDF2
import re
import pandas as pd



if __name__ == "__main__":
    # creating a pdf file object
    pdfFileObj = open('data/monitor_2022_153.pdf', 'rb')

    # creating a pdf reader object
    pdfReader = PyPDF2.PdfFileReader(pdfFileObj)

    # printing number of pages in pdf file
    print(pdfReader.numPages)

    data = []

   # for i in range(152, 196):
    for i in range(152, 196):
        text = pdfReader.getPage(i).extractText()
        for match in re.finditer('Poz.', text):
            str = text[match.start():match.start()+200]

            str = str.replace("\n", " ")
            #598028
            ind = str.find('KRS')
            print(str[:ind+16])
            print(str[:ind+10])
            #string = str[:ind+160]
            #print(string)
            #string = string.replace("\n", "")asd
            #
            #print(string)
            data.append(str[:ind+16])
            #print(match.start(), match.end())
            #print(str[:ind+16])

        #[m.start() for m in re.finditer('Poz.', text)]
        #ind = text.find('Poz.')
        #if ind != -1:
            #print(text[ind:ind+100])

        #print(text)

    df = pd.DataFrame(data=data)
    df.to_csv("data.csv", encoding='utf-16', index=False)


    print(df)
    #data.to_csv('KRS.csv')
    # creating a page object
    # pageObj = pdfReader.getPage(0)
    #
    # # extracting text from page
    # print(pageObj.extractText())

    # closing the pdf file object
    pdfFileObj.close()
