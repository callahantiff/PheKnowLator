import ftplib
import gzip
import os.path
import requests
import responses
import shutil
import unittest
import urllib3

from contextlib import closing
from urllib.request import urlopen

from pkt_kg.utils import *


class TestDataUtilsDownloading(unittest.TestCase):
    """Class to test the downloading methods from the data utility script."""

    def setUp(self):

        # create temporary directory to store data for testing
        current_directory = os.path.dirname(__file__)
        dir_loc = os.path.join(current_directory, 'data/temp')
        self.dir_loc = os.path.abspath(dir_loc)
        os.mkdir(self.dir_loc)

        # create fake zipped data
        empty_zip_data = b'1F   8B  08  00  00  00  00  00  00  0B'

        with open(self.dir_loc + '/variant_summary.txt.gz', 'wb') as zp:
            zp.write(empty_zip_data)

        content = b'Lots of content here'
        with gzip.open(self.dir_loc + '/variant_summary.txt.gz', 'wb') as f:
            f.write(content)

        # create some fake ftp data
        with open(self.dir_loc + '/hgnc_complete_set.txt', 'w') as file:
            file.write('None')

        # set some urls
        self.url = 'https://proconsortium.org/download/current/promapping.txt'
        self.ftp_url = 'ftp://ftp.ebi.ac.uk/pub/databases/genenames/new/tsv/hgnc_complete_set.txt'
        self.gzipped_ftp_url = 'ftp://ftp.ncbi.nlm.nih.gov/pub/clinvar/tab_delimited/variant_summary.txt.gz'
        self.zipped_url = 'https://reactome.org/download/current/ReactomePathways.gmt.zip'
        self.gzipped_url = 'https://www.disgenet.org/static/disgenet_ap1/files/downloads/disease_mappings.tsv.gz'

        # set write location
        self.write_location = self.dir_loc + '/'

        return None

    @responses.activate
    def test_url_download_200(self):
        """Tests url_download method when returning a 200 status."""

        # filename
        filename = self.url.split('/')[-1]

        # fake file connection
        responses.add(
            responses.GET,
            self.url,
            body='test',
            status=200,
            content_type='text/plain',
            headers={'Content-Length': '1200'}
            )

        # test mocked download
        r = requests.get(self.url, allow_redirects=True)
        self.assertTrue(r.ok)

        # test writing data
        downloaded_data = open(self.write_location + '{filename}'.format(filename=filename), 'wb')
        downloaded_data.write(r.content)
        downloaded_data.close()
        self.assertTrue(os.path.exists(self.write_location + filename))

        return None

    @responses.activate
    def test_url_download_404(self):
        """Tests url_download method when not returning a 200 status."""

        # fake file connection
        responses.add(
            responses.GET,
            self.url,
            body='test',
            status=400,
            content_type='text/plain',
            headers={'Content-Length': '1200'}
        )

        # test mocked download
        r = requests.get(self.url, allow_redirects=True)
        self.assertFalse(r.ok)

        return None

    @responses.activate
    def test_ftp_url_download(self):
        """Tests ftp_url_download method."""

        # filename
        filename = self.ftp_url.split('/')[-1]

        # fake file connection
        #     responses.add(
        #         responses.GET,
        #         self.ftp_url,
        #         body='test',
        #         status=200,
        #         content_type='text/plain',
        #         headers={'Content-Length': '1200'}
        #         )
        #
        #     # test mocked download
        #     with closing(urlopen(self.ftp_url)) as r:
        #         with open(self.write_location + '{filename}'.format(filename=filename), 'wb') as f:
        #             shutil.copyfileobj(r, f)
        #     r.close()

        # test mocked download
        self.assertTrue(os.path.exists(self.write_location + filename))

        return None

    @responses.activate
    def test_gzipped_ftp_url_download(self):
        """Tests gzipped_ftp_url_download method."""

        # get ftp server info
        file = self.gzipped_ftp_url.replace('ftp://', '').split('/')[-1]
        write_loc = self.write_location + '{filename}'.format(filename=file)

        # read in gzipped file,uncompress, and write to directory
        with gzip.open(write_loc, 'rb') as fid_in:
            with open(write_loc.replace('.gz', ''), 'wb') as f:
                f.write(fid_in.read())
        fid_in.close()

        # change filename and remove gzipped and original files
        os.remove(write_loc)

        # test mocked download
        self.assertFalse(os.path.exists(write_loc))
        self.assertTrue(os.path.exists(write_loc[:-3]))

        return None

    @responses.activate
    def test_zipped_url_download_200(self):
        """Tests zipped_url_download method when returning a 200 status."""

        # filename
        filename = self.zipped_url.split('/')[-1]

        # fake file connection
        responses.add(
            responses.GET,
            self.zipped_url,
            body='test',
            status=200,
            content_type='text/plain',
            headers={'Content-Length': '1200'}
        )

        # test mocked download
        r = requests.get(self.zipped_url, allow_redirects=True)
        self.assertTrue(r.ok)

        # test writing data
        downloaded_data = open(self.write_location + '{filename}'.format(filename=filename[:-4]), 'wb')
        downloaded_data.write(r.content)
        downloaded_data.close()

        self.assertFalse(os.path.exists(self.write_location + filename))
        self.assertTrue(os.path.exists(self.write_location + filename[:-4]))

        return None

    @responses.activate
    def test_zipped_url_download_400(self):
        """Tests zipped_url_download method when not returning a 200 status."""

        # fake file connection
        responses.add(
            responses.GET,
            self.zipped_url,
            body='test',
            status=400,
            content_type='text/plain',
            headers={'Content-Length': '1200'}
        )

        # test mocked download
        r = requests.get(self.zipped_url, allow_redirects=True)
        self.assertFalse(r.ok)

        return None

    @responses.activate
    def test_gzipped_url_download_200(self):
        """Tests gzipped_url_download method when returning a 200 status."""

        # filename
        filename = self.gzipped_url.split('/')[-1]

        # fake file connection
        responses.add(
            responses.GET,
            self.gzipped_url,
            body=gzip.compress(b'test data'),
            status=200,
            content_type='gzip',
            headers={'Content-Length': '1200'}
        )

        # test mocked download
        r = requests.get(self.gzipped_url, allow_redirects=True)
        self.assertTrue(r.ok)

        # test writing data
        with open(self.write_location + '{filename}'.format(filename=filename[:-3]), 'wb') as outfile:
            outfile.write(gzip.decompress(r.content))
        outfile.close()

        self.assertFalse(os.path.exists(self.write_location + filename))
        self.assertTrue(os.path.exists(self.write_location + filename[:-3]))

        return None

    @responses.activate
    def test_gzipped_url_download_400(self):
        """Tests gzipped_url_download method when not returning 200 status."""

        # fake file connection
        responses.add(
            responses.GET,
            self.gzipped_url,
            body=gzip.compress(b'test data'),
            status=400,
            content_type='gzip',
            headers={'Content-Length': '1200'}
        )

        # test mocked download
        r = requests.get(self.gzipped_url, allow_redirects=True)
        self.assertFalse(r.ok)

        return None

    def test_data_downloader(self):
        """Tests data_downloader method."""

        # url data
        data_downloader(self.url, self.write_location)
        self.assertTrue(os.path.exists(self.write_location + self.url.split('/')[-1]))

        # # ftp url data
        data_downloader(self.ftp_url, self.write_location)
        self.assertTrue(os.path.exists(self.write_location + self.ftp_url.split('/')[-1]))

        # gzipped ftp url data
        file = self.gzipped_ftp_url.replace('ftp://', '').split('/')[-1]
        write_loc = self.write_location + '{filename}'.format(filename=file)
        data_downloader(self.gzipped_ftp_url, self.write_location)
        self.assertTrue(os.path.exists(os.path.exists(write_loc[:-3])))

        # zipped data
        data_downloader(self.zipped_url, self.write_location)
        self.assertTrue(os.path.exists(self.write_location + self.zipped_url.split('/')[-1][:-4]))

        # gzipped data
        data_downloader(self.gzipped_url, self.write_location)
        self.assertTrue(os.path.exists(self.write_location + self.gzipped_url.split('/')[-1][:-3]))

        return None

    def tearDown(self):

        # remove temp directory
        shutil.rmtree(self.dir_loc)

        return None
