



class LogStore:
    def __init__(self):
        self.store=[]
        self.configs={}
    

    def storeReport(self,testID,metrics):
        self.store.append({testID:metrics})
        print(self.store)
    
    def storeConfigs(self,config):
        self.configs[config["testID"]]=config

    def getConfig(self,testID):
        return self.configs[testID]
    


