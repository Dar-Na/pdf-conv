import PyPDF2
import re
import pandas as pd
import requests
import os
import time


if __name__ == "__main__":
    dataPozKRS= []
    dataEmail = []
    dataWWW = []

    timeStart = time.time()

    path_of_the_directory = 'data'
    print("Files and directories in a specified path:")
    for filename in os.listdir(path_of_the_directory):
        f = os.path.join(path_of_the_directory, filename)
        if os.path.isfile(f):
            print('========================================')
            print('     FILE:', f)

            # creating a pdf file object
            pdfFileObj = open(f, 'rb')

            # creating a pdf reader object
            pdfReader = PyPDF2.PdfFileReader(pdfFileObj)

            # printing number of pages in pdf file
            print('     Pages: ', pdfReader.numPages)

            startPage = 0; endPage = 0

            for i in range(0, 20):
                text = pdfReader.getPage(i).extractText()
                for match in re.finditer('Wpisy pierwsze', text):
                    ind = text.find('Wpisy kolejne')
                    str = text[match.start():ind+16]
                    str = str.replace("\n", " ")
                    str = str.replace(" ", "")
                    #print(str)
                    ind = str.find('2.Wpisykolejne')
                    print('     Tekst: ', str)
                    print('     Page with need data: ', i + 1)
                    str2 = str[ind - 10:ind]
                    str2 = str2.replace("-", " ")
                    str2 = str2.replace(".", " ")
                    str2 = str2.split()
                    startPage = int(str2[0]);
                    endPage = int(str2[1])
                    print('     Start page to search: ', startPage)
                    print('     End page to search: ', endPage)
                    # for matchPageNumber in re.finditer('2.Wpisykolejne', str):
                    #     ind = matchPageNumber.start()
                    #     print('page number with need data: ', i+1)
                    #     str2 = str[matchPageNumber.start()-10:matchPageNumber.start()]
                    #     str2 = str2.replace("-", " ")
                    #     str2 = str2.replace(".", " ")
                    #     str2 = str2.split()
                    #     startPage = int(str2[0]); endPage = int(str2[1])
                    #     print('start page to search: ', startPage)
                    #     print('end page to search: ', endPage)
                    #     break

            timeStartFile = time.time()

            if startPage and endPage:
                for i in range(startPage, endPage):
                    text = pdfReader.getPage(i).extractText()
                    for match in re.finditer('Poz.', text):
                        str = text[match.start():match.start()+400]

                        str = str.replace("\n", " ")
                        ind = str.find('KRS')
                        KRS = str[ind+4:ind+14]
                        RODZAJ = 'P'
                        res = requests.get(f'https://api-krs.ms.gov.pl/api/krs/OdpisPelny/{KRS}?rejestr={RODZAJ}&format=json')

                        adresPocztyElektronicznej = ''
                        adresStronyInternetowej = ''

                        if (res):
                            resData = res.json()['odpis']['dane']['dzial1']

                            if 'siedzibaIAdres' in resData:
                                adresPocztyElektronicznej = resData['siedzibaIAdres']['adresPocztyElektronicznej']
                                adresStronyInternetowej = resData['siedzibaIAdres']['adresStronyInternetowej']

                            if 'siedzibaIAdresPodmiotuZagranicznego' in resData:
                                adresPocztyElektronicznej = resData['siedzibaIAdresPodmiotuZagranicznego']['adresPocztyElektronicznej']
                                adresStronyInternetowej = resData['siedzibaIAdresPodmiotuZagranicznego']['adresStronyInternetowej']


                            if (adresStronyInternetowej):
                                adresStronyInternetowej = adresStronyInternetowej[0]['adresStronyInternetowej']
                            else:
                                adresStronyInternetowej = ''

                            if (adresPocztyElektronicznej):
                                adresPocztyElektronicznej = adresPocztyElektronicznej[0]['adresPocztyElektronicznej']
                            else:
                                adresPocztyElektronicznej = ''

                        dataPozKRS.append(str[:ind+16])
                        dataEmail.append(adresPocztyElektronicznej)
                        dataWWW.append(adresStronyInternetowej)

            # closing the pdf file object
            pdfFileObj.close()

            # printing data
            print('     FILE DONE:', f)
            print('     with time: %s seconds' % (time.time() - timeStartFile))

    # printing data
    print('====================')
    print('     ALL DONE:', f)
    print('     time: %s seconds' % (time.time() - timeStart))

    d1 = {'Name': dataPozKRS, 'Adres poczty elektronicznej': dataEmail, 'Adres strony internetowej': dataWWW}

    df = pd.DataFrame(d1)
    df.to_csv(
        r"data.csv",
        encoding='utf-16',
        index=False,
        columns=('Name', 'Adres poczty elektronicznej', 'Adres strony internetowej'),
        header=['Name', 'Adres poczty elektronicznej', 'Adres strony internetowej'],
        sep='|'
    )
    print(df)

    dataEmail = list(filter(None, dataEmail))
    dataEmail = list(dict.fromkeys(dataEmail))
    devEmail = pd.DataFrame(dataEmail)
    devEmail.to_csv(
        r"Emails.csv",
        encoding='utf-16',
        index=False
    )
    print(devEmail)