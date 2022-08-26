import PyPDF2
import re
import pandas as pd
import requests
import os
import time
import multiprocessing as mp

path_of_the_directory = 'data'


def getKSR(files, q):
    dataPozKRS = []
    dataEmail = []
    dataWWW = []

    for filename in files:
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

            startPage = 0
            endPage = 0
            startSearchingPage = 0
            endSearchingPage = 20

            for i in range(startSearchingPage, endSearchingPage):
                text = pdfReader.getPage(i).extractText()
                for match in re.finditer('Wpisy pierwsze', text):
                    ind = text.find('Wpisy kolejne')
                    str = text[match.start():ind + 16]
                    str = str.replace("\n", " ")
                    str = str.replace(" ", "")
                    ind = str.find('2.Wpisykolejne')
                    str2 = str[ind - 10:ind]
                    str2 = str2.replace("-", " ")
                    str2 = str2.replace(".", " ")
                    str2 = str2.split()
                    startPage = int(str2[0]);
                    endPage = int(str2[1])

                    print('     Tekst: ', str)
                    print('     Page with need data: ', i + 1)
                    print('     Start page to search: ', startPage)
                    print('     End page to search: ', endPage)

            timeStartFile = time.time()

            if startPage and endPage:
                for i in range(startPage, endPage):
                    text = pdfReader.getPage(i).extractText()
                    for match in re.finditer('Poz.', text):
                        str = text[match.start():match.start() + 400]

                        str = str.replace("\n", " ")
                        ind = str.find('KRS')
                        KRS = str[ind + 4:ind + 14]
                        RODZAJ = 'P'
                        res = requests.get(
                            f'https://api-krs.ms.gov.pl/api/krs/OdpisPelny/{KRS}?rejestr={RODZAJ}&format=json')

                        adresPocztyElektronicznej = ''
                        adresStronyInternetowej = ''

                        if (res):
                            resData = res.json()['odpis']['dane']['dzial1']

                            if 'siedzibaIAdres' in resData:
                                adresPocztyElektronicznej = resData['siedzibaIAdres']['adresPocztyElektronicznej']
                                adresStronyInternetowej = resData['siedzibaIAdres']['adresStronyInternetowej']

                            if 'siedzibaIAdresPodmiotuZagranicznego' in resData:
                                adresPocztyElektronicznej = resData['siedzibaIAdresPodmiotuZagranicznego'][
                                    'adresPocztyElektronicznej']
                                adresStronyInternetowej = resData['siedzibaIAdresPodmiotuZagranicznego'][
                                    'adresStronyInternetowej']

                            if (adresStronyInternetowej):
                                adresStronyInternetowej = adresStronyInternetowej[0]['adresStronyInternetowej']
                            else:
                                adresStronyInternetowej = ''

                            if (adresPocztyElektronicznej):
                                adresPocztyElektronicznej = adresPocztyElektronicznej[0]['adresPocztyElektronicznej']
                            else:
                                adresPocztyElektronicznej = ''

                        dataPozKRS.append(str[:ind + 16])
                        dataEmail.append(adresPocztyElektronicznej)
                        dataWWW.append(adresStronyInternetowej)

            # closing the pdf file object
            pdfFileObj.close()
            # printing data
            print('     FILE DONE: ', f)
            print('     with time: %s seconds' % (time.time() - timeStartFile))

    print('PROCESS DONE!!!')
    print('PROCESS DONE!!!')
    print('PROCESS DONE!!!')
    q.put([dataPozKRS, dataEmail, dataWWW])


if __name__ == "__main__":
    ctx = mp.get_context('spawn')
    q = ctx.Queue()
    files = []
    N = 10

    dataPozKRS = []
    dataEmail = []
    dataWWW = []

    processes = []

    timeStart = time.time()

    # Creates N processes then starts them
    print("Files and directories in a specified path:")
    num = round(len(os.listdir(path_of_the_directory)) / N)
    print(num)
    tmp = []
    for i in range(0, len(os.listdir(path_of_the_directory))):
        if ((i + 1) % num == 0) or i == len(os.listdir(path_of_the_directory)) - 1:
            tmp.append(os.listdir(path_of_the_directory)[i])
            files.append(tmp)
            tmp = []
        else:
            tmp.append(os.listdir(path_of_the_directory)[i])

    # for i in range(0, N):
    #     print(files)
    #     print(files[i])
    #     print(len(files[i]))
    #     p = [mp.Process(target=getKSR, args=[file, q]) for file in files]
    #
    #     p = mp.Process(target=getKSR, args=(files[i], q))
    #     p.start()
    #     processes.append(p)
    for file in files:
        process = ctx.Process(target=getKSR, args=(file, q))
        processes.append(process)

    for process in processes:
        process.start()
        print('process starts with pid: ', process.pid)

    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')
    print('ALERT!!')

    print('DONE')
    print('DONE')
    print('DONE')
    print('DONE')

    for i in range(0, N):
        arr = q.get()
        dataPozKRS.extend(arr[0])
        dataEmail.extend(arr[1])
        dataWWW.extend(arr[2])
        print(dataPozKRS)
        print(dataEmail)
        print(dataWWW)

    for process in processes:
        if process.is_alive():
            print('process wait with pid: ', process.pid)
            process.join()
            print('process done with pid: ', process.pid)
        else:
            print('process ends with pid: ', process.pid)





    # printing data
    print('*===================')
    print('||                  ')
    print('||     ALL DONE')
    print('||     time: %s seconds' % (time.time() - timeStart))
    print('||                  ')
    print('*===================')

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
