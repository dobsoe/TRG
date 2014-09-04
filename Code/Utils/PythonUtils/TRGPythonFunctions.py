 # Converts front end Lotus Python Functions in generic PsyConn calls
# generic imports
import HelperFunctions
import PsyConn
import matplotlib.pyplot as plt

class FrontEnd(PsyConn.ConnectionFunctions): 
    
    def __init__(self):
        self.psyconn=[]
        self.helper=HelperFunctions.Helper()
        self.conn = []
        self.cur = []

    def ConnectToDCA(self):
        self.connect("localhost", "9999", "demo", "gpadmin")

    # misc plotting functions
    def Plot_ProductRefreshes(self):
        vals=self.executeandfetch("select sum(num), sum_wasrefreshed from seller_master group by 2 order by 2") 
        freq, refreshed=zip(*vals)
        np.histogram(freq, refreshed, freq, 'bo-')
        #plt.yscale('log')
        #plt.xlabel('# refreshes')
        #plt.ylabel('Frequency')
        plt.show()
