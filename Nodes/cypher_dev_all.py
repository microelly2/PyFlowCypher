
from nodeeditor.utils import *
from nodeeditor.say import *


# https://neo4j.com/docs/api/python-driver/current/#installation
from neo4j import GraphDatabase


def createNode2(tx,kid,typ,params):
        cmd="CREATE (p:"+typ+" {$params})"
        return tx.run(  "CREATE (p:"+typ+")"
                        "SET p.a='NEU' "
                       "SET p = $params " 
                        "RETURN p",                    
                       
        kid=kid,typ=typ,params=params)

from PyFlow.Core.Common import *

class Development:
    
    def run_Cypher_Driver(self):
        #uri = "bolt://localhost:7687"
        uri=self.getData('uri')
        user=self.getData('user')
        password=self.getData('password')
        say(uri,user,password)
        
        try:
            #uri = "bolt://localhost:7687"
            #driver = GraphDatabase.driver(uri, auth=("neo4j", "password"))

            driver = GraphDatabase.driver(uri, auth=(user, password))
            say(driver)
            self.setPinObject('driver',driver)

        except Exception as ex:
            sayErr(ex)


    def run_Cypher_Session(self):
        sayl()
        
        driver=self.getPinObject('driver')
        if driver is None:
            driver = GraphDatabase.driver("bolt://localhost:7687", auth=("neo4j", "password"))

        cmd=self.getData('cypher')

        say(cmd)

        if self.getData('useData'):
                sayW("USE Data")
                say(self.getData('data'))
                
                data=self.getData('data')
                for r in data:
                    say(r)

                    # cmd="MERGE (a:TEST{pid:$data[1],ip:$data[0]}) return (a)"
                    # MERGE (a:TEST{uid:$data[1],name:$data[2],ip:$data[0]}) return a
                    # MERGE (a:FoUser{uid:$data[1]}) Set a.name = $data[2] return a
                    # MERGE (a:FoIP{ip:$data[0]}) Set a.info = 'https://ipinfo.io/'+$data[0], a.test="1" return a
                    # MERGE (a:FoIP{ip:$data[0]})<-[r:from]- (b:FoUser{uid:$data[1]}) return r
                    
                    try:
                        with driver.session() as session:
                            rc = session.run(cmd,data=r)
                            say(rc.data())
                    
                    except Exception as e:
                        sayErr(e)
                        sayErr("Record:")
                        sayErr(r)
                        
                return


        # cmd="MATCH (a) RETURN a LIMIT 2"
        # MATCH (a:b_spline_curve_with_knots) RETURN a
        else:
            with driver.session() as session:
                 rc = session.run(cmd)
                        
        #say(rc) # <neo4j.BoltStatementResult object at 0x2ae4564fdef0>
        
        if self.getData('boltresultOnly'):       
            self.setPinObject("boltResult",rc)
            return None

        dat=rc.data()
        say("dat")
        say(dat)


        if len(dat)==0:
            sayW("no returs")
            return

        
        outArray = PFDict("StringPin")
        
        
        if len(dat)==1:

            enti=dat[0]
            say("enti-",enti)
            say(enti.keys())
            say("-----------")
            if len(enti.keys())==1:
                say("simple list")
                vals=[]
                for enti in dat:
                    vals += enti.values()
                say(vals)
                say("##########")
                self.setData('outList',vals)
                self.setData('outDict',outArray)
                return

            else:
                say("SEtzen")
                enti=dat[0]
                tt=enti.values()
                say(tt)
                for t in tt:
                    say("*",t)
                    obj=t
                
                for k,v in zip(obj.keys(),obj.values()):
                    outArray[k]=str(v)


                self.setData('outDict',outArray)
                self.setData('outList',dat)
                say("HUHU")
                return
        else:
            enti=dat[0]
            say("enti-",enti)
            say(enti.keys())
            say("-----------")
            if len(enti.keys())==1:
                say("simple list")
                vals=[]
                for enti in dat:
                    vals += enti.values()
                say(vals)
                say("##########")
                self.setData('outList',vals)
                self.setData('outDict',outArray)
                return
            else:
                obj=dat[0]['a']
                FreeCAD.obj=obj
                
                say(dat)
                say(obj)
                
                say("len dat",len(dat))
                say("obj keys vals ...")
                for k,v in zip(obj.keys(),obj.values()):
                    say(k,v)
                    
                self.setData('outDict',outArray)
                self.setData('outList',dat)
            
        
        '''
        >>> FreeCAD.obj.keys()
        dict_keys(['name', 'year'])
        >>> FreeCAD.obj.values()
        dict_values(['Anton', 2020])
        >>> FreeCAD.obj.graph
        <neo4j.types.graph.Graph object at 0x2b89b2ac0e48>
        >>> FreeCAD.obj.graph.nodes
        <neo4j.types.graph.EntitySetView object at 0x2b89b2a669e8>
        '''
    
        


    def run_Cypher_LoadCSV(self):

        driver=self.getPinObject('driver')
        cmd=self.getData('command')
        filename=self.getData('filename')
        #filename="https://neo4j.com/docs/cypher-manual/4.0/csv/artists.csv"
        
        ft=self.getData('fieldTerminator')
        if self.getData('withHeaders'):
            cmdall="LOAD CSV WITH HEADERS FROM '{}' AS line FIELDTERMINATOR '{}' ".format(filename,ft)
        else:
            cmdall="LOAD CSV FROM '{}' AS line FIELDTERMINATOR '{}' ".format(filename,ft)
        cmdall += " " + cmd
        say(cmdall)
        
        
        with driver.session() as session:
             rc = session.run(cmdall)

                
        rst=''
        ids=[]
        for d in rc.data():
            say(d)
            ids += [d['id']]
            rst += "\n"+str(d)
        self.setData('ids',ids)


    def run_Cypher_Connect(self):

        driver=self.getPinObject('driver')
        #cmd=self.getData('command')

        sources=self.getData('sources')
        targets=self.getData('targets')
        rel=self.getData('type')

        say(sources)
        say(targets)

        cmdall='''
        
                UNWIND $sources as x 
                UNWIND $targets as y 
                WITH x,y
                
                MATCH (a),(b)
                WHERE id(a)=x and id(b)=y
                MERGE (a)-[t:{}]->(b)
                return (t)

        '''.format(rel)
        say(cmdall)
        
        with driver.session() as session:
             rc = session.run(cmdall,sources=sources,targets=targets)

        say(rc)
        FreeCAD.rc=rc
        return
                
        rst=''
        ids=[]
        if self.getData('process'):
            for d in rc.data():
                say(d)
                #ids += [d['id']]
                rst += "\n"+str(d)
            self.setData("resultString",rst)
            self.setData('ids',ids)
        else:
            self.setData("resultString",None)
            self.setPinObject('resultObject',rc)
            say("##",rc.data())
                        



    def run_Cypher_ResultToy(self):
        sayl()

        result=self.getPinObject('bolTResult')        
        a=result.summary()
        say(a.statement)
        say(a.server.address)
        data=result.data()

        self.setData('outString',str(data))
        return 
        


'''
MERGE (a{id:185})-[r:REL]->(b{id:185})

UNWIND [185,186,187] as x
WITH x MERGE (a{id:x})-[r:REL]->(b{id:185})


MATCH (a) WHERE a.name IN ['Jonny','Zarah','Serge']  RETURN id(a)

MATCH (a) RETURN id(a)

'''


