import os
from grab_new_psis import FileContainer, Local_Folder, SharePoint
from processors import PSI_OBS, update_cfr
import login

#Credentials
lan = login.lan
pw = login.pw
url_2016_1 = 'https://intranet.mdlz.com/sites/rba/workstream_management/Pages/default.aspx?RootFolder=%2Fsites%2Frba%2Fworkstream%5Fmanagement%2FRBA%20Shared%20Docs%2FProduct%20Supply%20and%20Demand%20Planning%2FPSI%2FArchive%2F2016%20Lbs&FolderCTID=0x0120000CA5A2454372D94A902E5C4367173E73&View=%7bFCC2D600-00F0-4E78-9388-5B8337CD5313%7d'
url_2016_2 = 'https://intranet.mdlz.com/sites/rba/workstream_management/Pages/default.aspx?Paged=TRUE&p_SortBehavior=0&p_FileLeafRef=US%20PSI%20LBS%20Report%2007-29-2016%2exlsx&p_ID=3542&RootFolder=%2fsites%2frba%2fworkstream_management%2fRBA%20Shared%20Docs%2fProduct%20Supply%20and%20Demand%20Planning%2fPSI%2fArchive%2f2016%20Lbs&PageFirstRow=31&&View={FCC2D600-00F0-4E78-9388-5B8337CD5313}'
url_2015_1 = 'https://collaboration.mdlz.com/sites/productsupplysnackscerealsector/Cost%20Productivity%20%20KPIs/Forms/AllItems.aspx?RootFolder=%2Fsites%2Fproductsupplysnackscerealsector%2FCost%20Productivity%20%20KPIs%2FBiscuit%20Supply%20Planning%20updates%2FPSI%2F2015%20PSI%20Reports&FolderCTID=0x0120003F5CA948AC5D8D46AF4B01653321061F&View=%7BF4431E2C%2D7702%2D477B%2DBE30%2DBFDBF7BB1D21%7D&InitialTabId=Ribbon%2EDocument&VisibilityContext=WSSTabPersistence#InplviewHashf4431e2c-7702-477b-be30-bfdbf7bb1d21=RootFolder%3D%252fsites%252fproductsupplysnackscerealsector%252fCost%2520Productivity%2520%2520KPIs%252fBiscuit%2520Supply%2520Planning%2520updates%252fPSI%252f2015%2520PSI%2520Reports'
dl_dir = os.path.join(os.getcwd(),'psis')

if __name__ == "__main__":
    #Load latest CFR CSV
    update_cfr(os.path.join(os.getcwd(),'cfrdata.csv'))

    #Cycle through sites for PSIs
    SP = SharePoint(url_2016_1,lan,pw)
    SP.download_new_files(SP.new_downloads,directory=dl_dir)

    #Collect all files now in given path
    LF = Local_Folder(dl_dir)

    #Append new files to database
    for data in LF.file_queue:
        f_path = os.path.join(dl_dir,data)
        xl = PSI_OBS(f_path)
        LF.update_files(data)
        xl.burn_to_dataset(xl.psi_covdur,'full_set_2.csv')
        if xl.was_burned:
            LF.update_files(data)
