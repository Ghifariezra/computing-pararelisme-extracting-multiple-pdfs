from concurrent.futures import ProcessPoolExecutor
from itertools import chain, batched
from PIL import Image
from tqdm import tqdm
import fitz
import pytesseract
import pandas as pd
import os
import re
import time
import logging

logging.basicConfig(level=logging.INFO, filename='logs/Pararelisme.log', filemode='w')

pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'

def findFiles():
    dirStart = r'G:'
    dirFinal = 'pdfs'
    
    path = [
        roots 
        for roots, dirs, file in os.walk(dirStart) 
        if dirFinal in roots
    ] # Find master directory
    
    pathDir = [
        p.path 
        for p in os.scandir(path[0])
    ] # Find all files in the specified directory
    
    return pathDir

def extractPDF(files):
    pattRegu = r'NOMOR\s*:\s*(\d{1,3})'
    
    regulations = []
    applicable = []
    sanction = []

    for file in files:
        logging.info(f"PID: {os.getpid()}")
        start = time.time()

        pdfs = fitz.open(file)
        pages = pdfs.page_count

        # Loop through each page of the PDF
        for page in range(pages):
            try:
                loadDoc = pdfs.load_page(page) # Loading pages of a PDF document
                loadPageImage = loadDoc.get_pixmap(
                    colorspace = 'RGB'
                ) # Get an image in pixmap format from the page
                
                pageIMG = Image.frombytes(
                    mode = 'RGB',
                    size = [loadPageImage.width, loadPageImage.height], 
                    data = loadPageImage.samples
                ) # Create an image object from binary image data
                
                extractPageIMG = pytesseract.image_to_string(
                    image = pageIMG
                ) # Extract text from the image
                
                # Filter Data by Regulation, Berlaku Sejak and Sanksi
                regu = re.findall(
                    pattern=pattRegu,
                    string=extractPageIMG
                )
                berlakuSejak = re.findall(
                    pattern='berlaku sejak',
                    string=extractPageIMG
                )
                sanksi = re.findall(
                    pattern='sanksi',
                    string=extractPageIMG
                )

                filteredRegu = list(filter(None, regu))
                filteredApplicable = list(filter(None, berlakuSejak))
                filteredSanction = list(filter(None, sanksi))

                if page == 0 and filteredRegu:
                    regulations.append(f"Peraturan {filteredRegu[0]}")
                if page > 0 and filteredApplicable:
                    applicable.append(filteredApplicable[0])
                if page > 0 and filteredSanction:
                    sanction.append(filteredSanction[0])

            except Exception as e:
                logging.error(e)

        pdfs.close()
        end = time.time()
        duration = end - start
        logging.info(f'PID: {os.getpid()} | Time: {duration}')

    words = list(chain(applicable, sanction))

    if regulations and words:
        try: # If words just have one items
            frame = {
                'Words': words,
                'Regulations': regulations
            }

            logging.info(frame)
            return pd.DataFrame(frame)
        
        except ValueError: # If words have more than one item
            indexSanction = words.index('sanksi')
            frame = {
                'Words': words[indexSanction],
                'Regulations': regulations
            }

            logging.info(frame)
            return pd.DataFrame(frame)

def saveFrame(frames):
    # Merge all the dataframes
    combineDF = pd.concat(frames, ignore_index=True)
    logging.info(combineDF)
    
    if os.path.exists('result-data.csv'):
        combineDF.to_csv('data/result-data.csv', mode='w', index=False)
        print("Output CSV file saved successfully!")
    else:
        combineDF.to_csv('data/result-data.csv', index=False)
        print("Output CSV file saved successfully!")

def main():
    process = os.cpu_count()

    files = findFiles()

    # Create a file partition based on the number of processes, if the partition exceeds the number of processes in this code I add it to the Pool manager
    countChunk = len(files) // process 
    chunkDocs = list(
        batched(
            files,
            countChunk
        )
    ) # Create partition of files

    dfs = [] # Collect all the frame data from Extract PDF
    with ProcessPoolExecutor(max_workers=process + 1) as executor:
        results = list(
            tqdm(
                executor.map(extractPDF, chunkDocs),
                 total=len(chunkDocs)
            )
        )
        
        resultDF = [
            result 
            for result in results
        ]
        
        dfs.extend(resultDF)

    saveFrame(dfs)

if __name__ == "__main__":
    start = time.time()

    main()

    end = time.time()
    duration = end - start
    logging.info(f'Total Time: {duration}')