#Get New Weekly Data
import requests, os, re, csv, pickle
from urllib.parse import urlparse
from requests_ntlm import HttpNtlmAuth
from bs4 import BeautifulSoup as bs
from bs4 import SoupStrainer
from requests.packages.urllib3.exceptions import InsecureRequestWarning

class FileContainer(object):
    if not os.path.exists(os.path.join(os.getcwd(),'files.pickle')):
        with open(os.path.join(os.getcwd(),'files.pickle'), 'wb') as pk:
            pickle.dump(set(' '),pk)

    with open(os.path.join(os.getcwd(),'files.pickle'), 'rb') as pk:
        loaded_files = (pickle.load(pk))

    def __init__(self):
        self.loaded_files = loaded_files

    def new_files(self,fnames):
        return list(set(fnames).difference(self.loaded_files))

    def update_files(self,list_of_files):
        list_of_files = [list_of_files] if isinstance(list_of_files,str) else list_of_files
        update = set(list_of_files) | self.loaded_files
        with open(os.path.join(os.getcwd(),'files.pickle'), 'wb') as pk:
            pickle.dump(update,pk)
        with open(os.path.join(os.getcwd(),'files.pickle'), 'rb') as pk:
            self.loaded_files = (pickle.load(pk))

    def clear_file(self):
        with open(os.path.join(os.getcwd(),'files.pickle'), 'wb') as pk:
            pickle.dump(set(' '),pk)
        self.loaded_files = set(' ')
        print ('Pickled Loaded files cleared.')

class Local_Folder(FileContainer):
    def __init__(self, dirpath):
        self.dirpath = (os.path.join(os.getcwd(),dirpath))
        self.file_queue = self.new_files(self.list_files())

    def list_files(self):
        all_files = [f for f in os.listdir(self.dirpath) if f.endswith(('.xlsx','.xlsm','.xls'))]
        return all_files

    def record_file(self, file_name):
        with open(os.path.join(self.dirpath,self.txt_file), 'a') as txt:
            txt.write('{}{}'.format('\n',file_name))

class SharePoint(FileContainer):
    def __init__(self,url,username,password):
        self.url = url
        self._username = username
        self._password = password
        full_links, self.links = self.get_links()
        self.file_queue = self.new_files(self.links)
        self.new_downloads = [s for s in full_links if s.split('/')[-1] in self.file_queue]

    def get_links(self):
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        r = requests.get(self.url, auth=HttpNtlmAuth('KRFT.net\\{}'.format(self._username),'{}'.format(self._password)), verify=False)
        x = bs(r.content, "html.parser", parse_only=SoupStrainer('a'))
        links = [str(a['href']) for a in x if a.has_attr('href')]
        xl_links = [l for l in links if all(['PSI' in l, l.endswith(('.xlsx','.xlsm','.xls'))])]
        return xl_links, [x.split('/')[-1] for x in xl_links]

    def download_new_files(self,links, directory):
        domain = '{uri.scheme}://{uri.netloc}/'.format(uri=urlparse(self.url))
        requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
        dl_count = 0
        for l in links:
            path = '{}{}'.format(domain,l)
            stream = requests.get(path, auth=HttpNtlmAuth('KRFT.net\\{}'.format(self._username),'{}'.format(self._password)), stream=True, verify=False)
            with open(os.path.join(directory,'{}'.format(l.split('/')[-1])), 'wb') as f:
                for row in stream:
                    f.write(row)
            dl_count+=1
        print ('{} files downloaded'.format(dl_count))

#Credentials
path_of_files = "C:\\Users\\UNA0464\\Desktop\\all_psis\\"
