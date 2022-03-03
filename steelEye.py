#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
Module documentation
"""

from urllib.request import urlopen
import xml.etree.ElementTree as ET
from zipfile import ZipFile
from io import BytesIO
from bs4 import BeautifulSoup
import pandas as pd
import logging


def get_download_link(url):
    """
    this function will return zip download link 
    after parsing through the url provided 
    and checking for the first DLTINS file-type
    and fetching its download link
    
    :param url: xml url
    :return: zip download link
    """

    var_url = urlopen(url)

    tree = ET.parse(var_url)
    root_node = tree.getroot()

    lst = root_node.findall('./result/doc/str')

    for item in lst:
        if item.attrib['name'] == 'download_link':
            download_link = item.text
        if item.attrib['name'] == 'file_type' and item.text == 'DLTINS':
            break
    return download_link


def unzipper(download_link):
    """
    this function will return xml from  
    zip file url provided without writting
    it to disk
    
    :param download_link: zip download url
    :return: xml content
    """

    resp = urlopen(download_link)
    zipfile = ZipFile(BytesIO(resp.read()))
    foofile = zipfile.open('DLTINS_20210117_01of01.xml')
    xml_content = foofile.read()

    return xml_content


def get_final_df(final_xml_data):
    """
    this function will return final dataframe
    and processed xml is to be provided.
    
    :param final_xml_data: processed xml data
    :return: dataframe with final data
    """

    fin_block = final_xml_data.find_all('FinInstrmGnlAttrbts')
    issr_block = final_xml_data.find_all('Issr')

    _id = []
    fullNm = []
    clssfctnTp = []
    cmmdtyDerivInd = []
    ntnlCcy = []
    issr = []

    for i in range(len(fin_block)):
        temp_hdr = fin_block[i]

        _id.append(temp_hdr.find('Id').text)
        fullNm.append(temp_hdr.find('FullNm').text)
        clssfctnTp.append(temp_hdr.find('ClssfctnTp').text)
        cmmdtyDerivInd.append(temp_hdr.find('CmmdtyDerivInd').text)
        ntnlCcy.append(temp_hdr.find('NtnlCcy').text)
        issr.append(issr_block[i].text)

    df = pd.DataFrame()
    df['Id'] = _id
    df['FullNm'] = fullNm
    df['ClssfctnTp'] = clssfctnTp
    df['CmmdtyDerivInd'] = cmmdtyDerivInd
    df['NtnlCcy'] = ntnlCcy
    df['issr'] = issr

    return df


if __name__ == "__main__":
    logging.basicConfig(filename='steel-eye.log',
                        format='%(asctime)s %(message)s', filemode='w')

    # Creating an object

    logger = logging.getLogger()

    # Setting the threshold of logger to DEBUG

    logger.setLevel(logging.DEBUG)

    xml_url = \
        """https://registers.esma.europa.eu/solr/esma_registers_firds_files/select?q=*&fq=publication_date:%5B2021-01-17T00:00:00Z+TO+2021-01-19T23:59:59Z%5D&wt=xml&indent=true&start=0&rows=100"""

    try:
        logger.info('Retrieving zip download link')
        download_link = get_download_link(xml_url)

        logger.info('Download link ready. Streaming zip data.')
        xml_content = unzipper(download_link)
        logger.info('Unzipping done and XML ready')

        logger.info('Converting the XML to parsable format')
        final_xml_data = BeautifulSoup(xml_content, 'xml')

        logger.info('Sending the new XML to convert into desired dataframe'
                    )
        df = get_final_df(final_xml_data)

        logger.info('Sending CSV to S3')

        df.to_csv('s3://{}/{}'.format(bucket_name, 'steel-eye.csv'
                  ), storage_options={'key': <ENTER AWS KEY>,
                  'secret': <ENTER AWS SECRET>})
    except Exception as e:
        logger.error(str(e))
