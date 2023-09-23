import sqlite3
import time
import pandas as pd
import random
import datetime

class DataModel():
    '''Κλάση σύνδεσης με τη βάση δεδομένων και δημιουργίας δρομέα'''
    def __init__(self, filename):
        self.filename = filename
        try:
            self.con = sqlite3.connect(filename)
            self.con.row_factory = sqlite3.Row  # ώστε να πάρουμε τα ονόματα των στηλών του πίνακα
            self.cursor = self.con.cursor()
            print("Επιτυχής σύνδεση στη βάση δεδομένων", filename)
            sqlite_select_Query = "select sqlite_version();"
            self.cursor.execute(sqlite_select_Query)
            record = self.cursor.fetchall()
            for rec in record:
                print("SQLite Database Version is: ", rec[0])
        except sqlite3.Error as error:
            print("Σφάλμα σύνδεσης στη βάση δεδομένων sqlite σφάλμα", error)
        
        t1 = time.perf_counter()
        
        #create and insert into database
        '''
        self.create_tables()
        self.people, self.address = self.open_csvs()
        self.insert_into_ekdilosi()
        self.insert_into_pelatis()
        self.insert_into_sintelestis()
        self.insert_into_xoros()
        self.xoroi = self.find_all_xoroi()
        self.insert_into_zoni()
        self.zones = self.find_all_zones()
        self.insert_into_thesi()
        self.ekdilosis = self.find_all_ekdilosis()
        self.insert_into_dieksagogi()
        self.xoroi_dieksagogon = self.find_all_xoroi_dieksagogon()
        self.insert_into_anathesi_timis()
        self.pelates = self.find_all_dieksagoges()
        self.dieksagoges = self.find_all_dieksagoges()
        self.insert_into_afora()
        self.sintelestes = self.find_all_sintelestes()
        self.insert_into_simetexei()
        '''
        self.drop_indexes()
        
        sql_time = time.perf_counter() - t1
        print(f"Συνολικός χρόνος: {sql_time:.5f}")
    
    def close(self):
        self.con.commit()
        self.con.close()
    
    def open_csvs(self):
        
        t1 = time.perf_counter()
        
        people = []
        address = []
        
        try:
            people_df = pd.read_csv('people.csv', sep='\t')
            address_df = pd.read_csv('address.csv', sep=',')
            
            for people_line in people_df.iterrows():
                    people.append(people_line[1][1])
            people = list(set(people))
            
            for address_line in address_df.iterrows():
                arithmos = address_line[1][0]
                dromos = address_line[1][1]
                poli = address_line[1][2]
                tk = address_line[1][3]
                address.append(arithmos + ',' + dromos + ',' + poli + ',' + tk)
            
            sql_time = time.perf_counter() - t1
            
            print(f"Επιτυχής άνοιγμα των αρχείων people.csv και address.csv χρόνος: {sql_time:.5f}")
            
        except:
            print("Σφάλμα με τα αρχεία people.csv και address.csv")

        return people, address
    
    def create_tables(self):
        
        t1 = time.perf_counter()
        
        try:
            self.cursor.execute('''CREATE TABLE pelatis (
                `id_pelati` integer NOT NULL UNIQUE PRIMARY KEY AUTOINCREMENT,
                `onomateponimo` varchar(100) NOT NULL,
                `dromos` varchar(100) NOT NULL,
                `arithmos` integer NOT NULL,
                `poli` varchar(100) NOT NULL,
                `T.K.` integer NOT NULL,
                `e_mail` varchar(100) NOT NULL UNIQUE,
                `tilefono` integer NOT NULL UNIQUE
                );''')

            self.cursor.execute('''CREATE TABLE kratisi (
                `id_kratisis` integer NOT NULL PRIMARY KEY AUTOINCREMENT,
                `date_time` datetime NOT NULL,
                `katastasi` boolean NOT NULL
                );''')

            self.cursor.execute('''CREATE TABLE kanei_kratisi (
                `id_pelati` integer NOT NULL,
                `id_kratisis` integer NOT NULL,
                CONSTRAINT `primary_key_kanei_kratisi` PRIMARY KEY (`id_pelati`, `id_kratisis`),
                CONSTRAINT `foreign_key_kanei_kratisi_pelatis` FOREIGN KEY (`id_pelati`) REFERENCES `pelatis` (`id_pelati`) ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT `foreign_key_kanei_kratisi_kratisi` FOREIGN KEY (`id_kratisis`) REFERENCES `kratisi` (`id_kratisis`) ON DELETE CASCADE ON UPDATE CASCADE
                );''')

            self.cursor.execute('''CREATE TABLE ekdilosi (
                `id_ekdilosis` integer NOT NULL UNIQUE PRIMARY KEY AUTOINCREMENT,
                `onoma` varchar(100) NOT NULL,
                `eidos` varchar(100) NOT NULL,
                `timh` integer NOT NULL,
                `diarkia` integer NOT NULL
                );''')

            self.cursor.execute('''CREATE TABLE dieksagogi (
                `date` date NOT NULL,
                `time` time NOT NULL,
                `id_ekdilosis` integer NOT NULL,
                `id_xorou` integer NOT NULL,
                `arithmos_probolis` integer NOT NULL DEFAULT 1,
                CONSTRAINT `primary_key_dieksagogi` PRIMARY KEY (`date`, `time`, `id_ekdilosis`, `id_xorou`),
                CONSTRAINT `foreign_key_dieksagogi_ekdilosi` FOREIGN KEY (`id_ekdilosis`) REFERENCES `ekdilosi` (`id_ekdilosis`) ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT `foreign_key_dieksagogi_xoros` FOREIGN KEY (`id_xorou`) REFERENCES `xoros` (`id_xorou`) ON DELETE CASCADE ON UPDATE CASCADE
                );''')

            self.cursor.execute('''CREATE TABLE xoros (
                `id_xorou` integer NOT NULL UNIQUE PRIMARY KEY AUTOINCREMENT,
                `onoma` varchar(100) NOT NULL,
                `dromos` varchar(100) NOT NULL,
                `arithmos` integer NOT NULL,
                `poli` varchar(100) NOT NULL,
                `T.K.` integer NOT NULL,
                `e_mail` varchar(100) NOT NULL UNIQUE,
                `tilefono` integer NOT NULL UNIQUE
                );''')

            self.cursor.execute('''CREATE TABLE zoni (
                `id_xorou` integer NOT NULL,
                `name` varchar(100) NOT NULL,
                CONSTRAINT `primary_key_zoni` PRIMARY KEY (`id_xorou`, `name`),
                CONSTRAINT `foreign_key_zoni_xoros` FOREIGN KEY (`id_xorou`) REFERENCES `xoros` (`id_xorou`) ON DELETE CASCADE ON UPDATE CASCADE
                );''')

            self.cursor.execute('''CREATE TABLE thesi (
                `id_xorou` integer NOT NULL,
                `id_zonis` integer NOT NULL,
                `seira` integer NOT NULL,
                `thesi` integer NOT NULL,
                CONSTRAINT `primary_key_thesi` PRIMARY KEY (`id_xorou`, `id_zonis`, `seira`, `thesi`),
                CONSTRAINT `foreign_key_thesi_xoros` FOREIGN KEY (`id_xorou`) REFERENCES `xoros` (`id_xorou`) ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT `foreign_key_thesi_zoni` FOREIGN KEY (`id_zonis`) REFERENCES `zoni` (`id_zonis`) ON DELETE CASCADE ON UPDATE CASCADE
                );''')

            self.cursor.execute('''CREATE TABLE afora (
                `id_kratisis` integer NOT NULL,
                `date` date NOT NULL,
                `time` time NOT NULL,
                `id_ekdilosis` integer NOT NULL,
                `id_xorou` integer NOT NULL,
                `id_zonis` integer NOT NULL,
                `seira` integer NOT NULL,
                `thesi` integer NOT NULL,
                `arithmos_theseon` integer NOT NULL DEFAULT 1 CHECK (arithmos_theseon <= 5),
                CONSTRAINT `primary_key_afora` PRIMARY KEY (`id_kratisis`, `date`, `time`, `id_ekdilosis`, `id_xorou`, `id_zonis`, `seira`, `thesi`),
                CONSTRAINT `foreign_key_afora_kratisi` FOREIGN KEY (`id_kratisis`) REFERENCES `kratisi` (`id_kratisis`) ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT `foreign_key_afora_date` FOREIGN KEY (`date`) REFERENCES `dieksagogi` (`date`) ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT `foreign_key_afora_time` FOREIGN KEY (`time`) REFERENCES `dieksagogi` (`time`) ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT `foreign_key_afora_ekdilosi` FOREIGN KEY (`id_ekdilosis`) REFERENCES `dieksagogi` (`id_ekdilosis`) ON DELETE CASCADE ON UPDATE CASCADE
                CONSTRAINT `foreign_key_afora_xorou` FOREIGN KEY (`id_xorou`) REFERENCES `thesi` (`id_xorou`) ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT `foreign_key_afora_zonis` FOREIGN KEY (`id_zonis`) REFERENCES `thesi` (`id_zonis`) ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT `foreign_key_afora_seira` FOREIGN KEY (`seira`) REFERENCES `thesi` (`seira`) ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT `foreign_key_afora_thesi` FOREIGN KEY (`thesi`) REFERENCES `thesi` (`thesi`) ON DELETE CASCADE ON UPDATE CASCADE
                );''')

            self.cursor.execute('''CREATE TABLE anathesi_timis (
                `id_ekdilosis` integer NOT NULL,
                `id_xorou` integer NOT NULL,
                `id_zonis` integer NOT NULL,
                `epipleon_timi` integer NOT NULL DEFAULT 0,
                CONSTRAINT `primary_key_anathesi_timis` PRIMARY KEY (`id_ekdilosis`, `id_xorou`, `id_zonis`),
                CONSTRAINT `foreign_key_anathesi_timis_ekdilosis` FOREIGN KEY (`id_ekdilosis`) REFERENCES `ekdilosi` (`id_ekdilosis`) ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT `foreign_key_anathesi_timis_xorou` FOREIGN KEY (`id_xorou`) REFERENCES `xoros` (`id_xorou`) ON DELETE CASCADE ON UPDATE CASCADE
                CONSTRAINT `foreign_key_anathesi_timis_zoni` FOREIGN KEY (`id_zonis`) REFERENCES `zoni` (`id_zonis`) ON DELETE CASCADE ON UPDATE CASCADE
                );''')

            self.cursor.execute('''CREATE TABLE sintelestis (
                `id_sintelesti` integer NOT NULL UNIQUE PRIMARY KEY AUTOINCREMENT,
                `onomateponimo` varchar(100) NOT NULL,
                `eidos` varchar(100) NOT NULL
                );''')

            self.cursor.execute('''CREATE TABLE simetexei (
                `id_ekdilosis` integer NOT NULL,
                `id_sintelesti` integer NOT NULL,
                `rolos` varchar(100) NOT NULL,
                CONSTRAINT `primary_key_anathesi_timis` PRIMARY KEY (`id_ekdilosis`, `id_sintelesti`),
                CONSTRAINT `foreign_key_sintelesti_ekdilosis` FOREIGN KEY (`id_ekdilosis`) REFERENCES `ekdilosi` (`id_ekdilosis`) ON DELETE CASCADE ON UPDATE CASCADE,
                CONSTRAINT `foreign_key_sintelesti_sintelesti` FOREIGN KEY (`id_sintelesti`) REFERENCES `sintelestis` (`id_sintelesti`) ON DELETE CASCADE ON UPDATE CASCADE
                );''')

            sql_time = time.perf_counter() - t1
            
            print(f"Επιτυχής δημιουργία των table της βάσης δεδομένων χρόνος: {sql_time:.5f}")
            
        except sqlite3.Error as error:
            print("Η βάση δεδομένων (sqlite) δεν δημιουργήθηκε σφάλμα: ", error)
    
    def insert_into_ekdilosi(self):
        
        t1 = time.perf_counter()
        
        try:
            df = pd.read_csv('movies.csv', sep='\t')
        except:
            print("Σφάλμα με το αρχείο movies.csv")
            
        try:
            genres = ["action", "adventure", "comedy", "theater"]
            i = 0
            for line in df.iterrows():
                
                genre = genres[random.randint( 0, ( len(genres) - 1 ) )]
                timh = random.randint(10,20)
                diarkia = random.randint(1,2)
                
                self.cursor.execute('''INSERT INTO `ekdilosi` (`onoma`, `eidos`, `timh`, `diarkia`) VALUES (?, ?, ?, ?);''',
                                    (line[1][1], genre, timh, diarkia))
                
                i+=1
                if(i == 100_000):
                    break
            
            sql_time = time.perf_counter() - t1
            
            print(f"Επιτυχής insert στo table ekdilosi χρόνος: {sql_time:.5f}")
            
        except sqlite3.Error as error:
            print("Στo table ekdilosi δεν έγινε insert σφάλμα: ", error)
            
        except:
            print("Στo table ekdilosi δεν έγινε insert")

    def insert_into_pelatis(self):
        
        t1 = time.perf_counter()
        
        try:
            for i in range(100_000):
                onomateponimo = self.people[i]
                
                arithmos, dromos, poli, tk = self.address[(i % len(self.address))].split(',')
                
                try:
                    e_mail = '_'.join(onomateponimo.split(' ')) + "@gmail.com"
                except:
                    e_mail = onomateponimo + "@gmail.com"
                
                e_mail = e_mail.strip()
                    
                tilefono = 69067318
                tilefono += i
                
                self.cursor.execute('''INSERT INTO `pelatis` (`onomateponimo`, `dromos`, `arithmos`, `poli`, `T.K.`, `e_mail`, `tilefono`) VALUES (?, ?, ?, ?, ?, ?, ?);''',
                                    (onomateponimo, dromos, arithmos, poli, tk, e_mail, tilefono))
            
            sql_time = time.perf_counter() - t1
            
            print(f"Επιτυχής insert στo table pelatis χρόνος: {sql_time:.5f}")
            
        except sqlite3.Error as error:
            print("Στo table pelatis δεν έγινε insert σφάλμα: ", error)
            
        except:
            print("Στo table pelatis δεν έγινε insert")
    
    def insert_into_sintelestis(self):
        
        t1 = time.perf_counter()
        
        try:     
            for i in range(500_000):
                onomateponimo = self.people[i + 100_000]
                
                eidos = "actor"
                
                self.cursor.execute('''INSERT INTO `sintelestis` (`onomateponimo`, `eidos`) VALUES (?, ?);''',
                                    (onomateponimo, eidos))
            
            sql_time = time.perf_counter() - t1
            
            print(f"Επιτυχής insert στo table sintelestis χρόνος: {sql_time:.5f}")
            
        except sqlite3.Error as error:
            print("Στo table sintelestis δεν έγινε insert σφάλμα: ", error)
                
        except:
            print("Στo table sintelestis δεν έγινε insert")
    
    def insert_into_xoros(self):
        
        t1 = time.perf_counter()
        
        try:
            for i in range(10_000):

                onoma = self.people[i + 200_000]
                arithmos, dromos, poli, tk = self.address[(i % len(self.address))].split(',')
                
                try:
                    e_mail = '_'.join(onoma.split(' ')) + "@gmail.com"
                except:
                    e_mail = onoma + "@gmail.com"
                
                e_mail = e_mail.strip()
                
                tilefono = 69068149
                tilefono += i

                self.cursor.execute('''INSERT INTO `xoros` (`onoma`, `dromos`, `arithmos`, `poli`, `T.K.`, `e_mail`, `tilefono`) VALUES (?, ?, ?, ?, ?, ?, ?);''',
                                    (onoma, dromos, arithmos, poli, tk, e_mail, tilefono))
            
            sql_time = time.perf_counter() - t1
            
            print(f"Επιτυχής insert στo table xoros χρόνος: {sql_time:.5f}")
            
        except sqlite3.Error as error:
            print("Στo table xoros δεν έγινε insert σφάλμα: ", error)
                
        except:
            print("Στo table xoros δεν έγινε insert")
    
    def find_all_xoroi(self):
        self.cursor.execute('''SELECT `id_xorou` FROM `xoros`;''')
        return self.cursor.fetchall()

    def insert_into_zoni(self):
        
        t1 = time.perf_counter()
        
        try:
            zones_names = ['A', 'B', 'C']

            for xoros in self.xoroi:
                id_xorou = int(xoros[0])
                arithmos_zonon = random.randint(1,3)
                
                for i in range(arithmos_zonon):
                    self.cursor.execute('''INSERT INTO `zoni` (`id_xorou`, `name`) VALUES (?, ?);''',
                                (id_xorou, zones_names[i]))
        
            sql_time = time.perf_counter() - t1
            
            print(f"Επιτυχής insert στo table zoni χρόνος: {sql_time:.5f}")
            
        except sqlite3.Error as error:
            print("Στo table zoni δεν έγινε insert σφάλμα: ", error)
                
        except:
            print("Στo table zoni δεν έγινε insert")

    def find_all_zones(self):
        self.cursor.execute('''SELECT `id_xorou`, `name`  FROM `zoni`;''')
        return self.cursor.fetchall()

    def insert_into_thesi(self):
        
        t1 = time.perf_counter()
        
        try:
            seires = [60, 70 , 80, 90, 100]
            thesis = [10, 20 , 30, 40, 50]

            for zoni in self.zones:
                id_xorou = zoni[0]
                id_zonis = zoni[1]
                
                aritmos_seiron = seires[random.randint(0, (len(seires)-1))]
                aritmos_theseon = thesis[random.randint(0, (len(thesis)-1))]
                
                for ar_seiras in range(1, (aritmos_seiron+1)):
                    for ar_thesis in range(1, (aritmos_theseon+1)):
                        self.cursor.execute('''INSERT INTO `thesi` (`id_xorou`, `id_zonis`, `seira`, `thesi`) VALUES (?, ?, ?, ?);''',
                                    (id_xorou, id_zonis, ar_seiras, ar_thesis))
            
            sql_time = time.perf_counter() - t1
            
            print(f"Επιτυχής insert στo table thesi χρόνος: {sql_time:.5f}")
            
        except sqlite3.Error as error:
            print("Στo table thesi δεν έγινε insert σφάλμα: ", error)
                
        except:
            print("Στo table thesi δεν έγινε insert")
    
    def find_all_ekdilosis(self):
        self.cursor.execute('''select `id_ekdilosis` from `ekdilosi`;''')
        return self.cursor.fetchall()

    # find next day
    def next_day(self, date_time, mera, diarkeia):
        
        date, time = date_time.split(' ')
        year, month, day = date.split('-')
        
        date = datetime.datetime(int(year), int(month), int(day)) + datetime.timedelta(days=mera)
        
        year = date.strftime('%Y')
        month = date.strftime('%m')
        day = date.strftime('%d')
        
        date = datetime.datetime(int(year), int(month), int(day), 20, 0, 0) + datetime.timedelta(hours=diarkeia)
        
        year = date.strftime('%Y')
        month = date.strftime('%m')
        day = date.strftime('%d')
        time = date.strftime("%H:%M:%S")
        date_time = year + '-' + month + '-' + day + ' ' + time
        
        return date_time

    def insert_into_dieksagogi(self):
        
        t1 = time.perf_counter()
        
        try:
            # pointer for xoroi
            i = 0
            for ekdilosi in self.ekdilosis:
                
                # initial date time
                date_time = '2023-01-01 20:00:00'
                date_time = self.next_day(date_time, ( i//len(self.xoroi) ) * 5 , 0) # to be faster
                
                proboles_per_day = random.randint(1, 2)
                number_of_days = random.randint(1, 5)
                id_xorou = self.xoroi[i % len(self.xoroi)][0]
                
                for l in range(number_of_days):
                    diarkeia = 0
                    for k in range(0, proboles_per_day):
                        
                        # find second start time
                        if (k == 1):
                            
                            # find duration
                            self.cursor.execute('''SELECT `diarkia` FROM `ekdilosi` WHERE id_ekdilosis = ?;''',
                                            (ekdilosi[0],))
                            
                            diarkeia = self.cursor.fetchall()[0][0]
                            
                            # find second start time
                            date_time = self.next_day(date_time, 0, diarkeia)                    
                        
                        self.cursor.execute('''INSERT INTO `dieksagogi` (`date`, `time`, `id_ekdilosis`, `id_xorou`, `arithmos_probolis`) VALUES (?, ?, ?, ?, ?);''',
                                    (date_time.split(' ')[0], date_time.split(' ')[1], ekdilosi[0], id_xorou, k+1))
                    
                    # next day
                    date_time = self.next_day(date_time, 1, 0)
                
                # next xoros
                i += 1

            sql_time = time.perf_counter() - t1
            
            print(f"Επιτυχής insert στo table dieksagogi χρόνος: {sql_time:.5f}")
            
        except sqlite3.Error as error:
            print("Στo table dieksagogi δεν έγινε insert σφάλμα: ", error)
                
        except:
            print("Στo table dieksagogi δεν έγινε insert")

    def find_all_xoroi_dieksagogon(self):
        self.cursor.execute('''SELECT DISTINCT id_ekdilosis, id_xorou, name FROM dieksagogi NATURAL JOIN zoni;''')
        return self.cursor.fetchall()

    def insert_into_anathesi_timis(self):
        
        t1 = time.perf_counter()
        
        try:
            for xoros_ekdiloseon in self.xoroi_dieksagogon:
                id_ekdilosis = xoros_ekdiloseon[0]
                id_xorou = xoros_ekdiloseon[1]
                id_zonis = xoros_ekdiloseon[2]
                
                epipleon_timh = random.randint(0,10)
                
                self.cursor.execute('''INSERT INTO `anathesi_timis` (`id_ekdilosis`, `id_xorou`, `id_zonis`, `epipleon_timi`) VALUES (?, ?, ?, ?);''',
                                (id_ekdilosis, id_xorou, id_zonis, epipleon_timh))
                
            sql_time = time.perf_counter() - t1
            
            print(f"Επιτυχής insert στo table anathesi_timis χρόνος: {sql_time:.5f}")
            
        except sqlite3.Error as error:
            print("Στo table anathesi_timis δεν έγινε insert σφάλμα: ", error)
                
        except:
            print("Στo table anathesi_timis δεν έγινε insert")
        
    def find_all_dieksagoges(self):
        self.cursor.execute('''SELECT id_pelati name FROM pelatis;''')
        return self.cursor.fetchall()

    def find_all_dieksagoges(self):
        self.cursor.execute('''SELECT `date`, `time`, `id_ekdilosis`, `id_xorou` FROM `dieksagogi`;''')
        return self.cursor.fetchall()

    def insert_into_afora(self):
        
        t1 = time.perf_counter()
        
        try:
            for i in range(30_000):
                # epilogi pelati
                id_pelati = self.pelates[random.randint(0, ( len(self.ekdilosis) - 1 ) )][0]
                
                # epilogi dieksagogis
                p = random.randint(0, ( len(self.dieksagoges) - 1 ) )    
                date = self.dieksagoges[p][0]
                my_time = self.dieksagoges[p][1]
                id_ekdilosis = self.dieksagoges[p][2]
                id_xorou = self.dieksagoges[p][3]
                
                # kenes thesis dieksagogis
                self.cursor.execute('''SELECT `id_zonis`, `seira`, `thesi` FROM `thesi` WHERE `id_xorou` = ?
                            EXCEPT
                            SELECT `id_zonis`, `seira`, `thesi` FROM afora 
                                WHERE `date` = ? AND `time` = ? AND `id_ekdilosis` = ? AND  id_xorou = ?;''',
                            (id_xorou, date, my_time, id_ekdilosis, id_xorou))
                kenes_thesis = self.cursor.fetchall()
                
                # kratisi
                self.cursor.execute('''INSERT INTO `kratisi` (`date_time`, `katastasi`) VALUES (?, ?);''',
                            (datetime.datetime.now(), 'true'))
        
                # key kratisis
                self.cursor.execute('''SELECT MAX(id_kratisis) FROM kratisi;''')
                id_kratisis = self.cursor.fetchall()[0][0]
                
                # kanei_kratisi
                self.cursor.execute('''INSERT INTO `kanei_kratisi` (`id_pelati`, `id_kratisis`) VALUES (?, ?);''',
                                (id_pelati, id_kratisis))
                
                number_of_thesis = random.randint(1, 5)
                p = random.randint(0, ( len(kenes_thesis) - 1 ) )
                for i in range(number_of_thesis):
                    
                    id_zonis = kenes_thesis[(p+i) % len(kenes_thesis)][0]
                    seira = kenes_thesis[(p+i) % len(kenes_thesis)][1]
                    thesi = kenes_thesis[(p+i) % len(kenes_thesis)][2]
                    
                    # afora i kratisi
                    self.cursor.execute('''INSERT INTO `afora` (`id_kratisis`, `date`, `time`,
                                `id_ekdilosis`, `id_xorou`, `id_zonis`, `seira`, `thesi`, arithmos_theseon) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?);''',
                                (id_kratisis, date, my_time, id_ekdilosis, id_xorou, id_zonis, seira, thesi, number_of_thesis))
                    
            sql_time = time.perf_counter() - t1
            
            print(f"Επιτυχής insert στα table kratisi, kanei_kratisi, afora χρόνος: {sql_time:.5f}")
            
        except sqlite3.Error as error:
            print("Στα table kratisi, kanei_kratisi, afora δεν έγινε insert σφάλμα: ", error)
                
        except:
            print("Στα table kratisi, kanei_kratisi, afora δεν έγινε insert")
    
    def find_all_sintelestes(self):
        self.cursor.execute('''SELECT `id_sintelesti` FROM `sintelestis`;''')
        return self.cursor.fetchall()

    def insert_into_simetexei(self):
        
        t1 = time.perf_counter()
        
        try:
            roloi = ['star', 'co-star']

            for i in range(100_000):
                id_ekdilosis = self.ekdilosis[i][0]
                
                for j in range(i*5, (i*5)+5):
                    id_sintelesti = self.sintelestes[j][0]
                    rolos = roloi[random.randint(0, 1)]
                    self.cursor.execute('''INSERT INTO `simetexei` (`id_ekdilosis`, `id_sintelesti`, `rolos`) VALUES (?, ?, ?);''',
                                (id_ekdilosis, id_sintelesti, rolos))
                    
            sql_time = time.perf_counter() - t1
            
            print(f"Επιτυχής insert στo table simetexei χρόνος: {sql_time:.5f}")
            
        except sqlite3.Error as error:
            print("Στo table simetexei δεν έγινε insert σφάλμα: ", error)
                
        except:
            print("Στo table simetexei δεν έγινε insert")

    def create_indexes(self, ind):
        
        t1 = time.perf_counter()
        
        try:
            if (ind == 0):
                self.cursor.execute('''CREATE INDEX `index_pelatis_1` ON `pelatis` (`e_mail`);''')
            elif (ind == 1):
                self.cursor.execute('''CREATE INDEX `index_pelatis_2` ON `pelatis` (`tilefono`);''')
            elif (ind == 2):
                self.cursor.execute('''CREATE INDEX `index_dieksagogi` ON `dieksagogi` (`date`);''')
            elif (ind == 3):
                self.cursor.execute('''CREATE INDEX `index_ekdilosi_1` ON `ekdilosi` (`onoma`);''')
            elif (ind == 4):
                self.cursor.execute('''CREATE INDEX `index_ekdilosi_2` ON `ekdilosi` (`eidos`);''')
            elif (ind == 5):
                self.cursor.execute('''CREATE INDEX `index_xoros_1` ON `xoros` (`e_mail`);''')
            elif (ind == 6):
                self.cursor.execute('''CREATE INDEX `index_xoros_2` ON `xoros` (`tilefono`);''')
            elif (ind == 7):
                self.cursor.execute('''CREATE INDEX `index_afora_1` ON `afora` (`id_xorou`);''')
            elif (ind == 8):
                self.cursor.execute('''CREATE INDEX `index_afora_2` ON `afora` (`date`);''')
            elif (ind == 9):
                self.cursor.execute('''CREATE INDEX `index_thesi` ON `thesi` (`id_xorou`);''')

            sql_time = time.perf_counter() - t1
                
            print(f"Επιτυχής δημιουργεία των indexes χρόνος: {sql_time:.5f}")
        
        except sqlite3.Error as error:
            print("Οι indexes δεν δημιουργήθηκαν σφάλμα: ", error)
            
    def select_random_value(self, table, row):
        try:
            query = '''SELECT COUNT(*), COUNT( DISTINCT ''' + row + ''') FROM ''' + table + ''';'''
            self.cursor.execute(query)
            values = self.cursor.fetchall()
            count1 = values[0][0]
            count2 = values[0][1]
            print("Αριθμός εγγραφών: " + str(count1))
            print("Διαφορετικές τιμές της στήλης " + row + ": " + str(count2))
            
            query = '''SELECT ''' + row + ''' FROM ''' + table + ''';'''
            self.cursor.execute(query)
            values = self.cursor.fetchall()
            value = values[random.randint( 0, ( len(values) - 1 ) )][0]
            
            if (type(value) is not str):
                value = str(value)
            
            print("Από την στήλη " + row + " του table " + table + " επιλέχθηκε η τιμή: " + value)
            
            return value
        
        except sqlite3.Error as error:
            print("Το value δεν βρέθηκε σφάλμα: ", error)
            return None
    
    def do_queries(self, table, row, row_value, value, ind, ind_num):
        
        if (ind == True):
            self.create_indexes(ind_num)
        
        try:
            query = '''SELECT ''' + row +''', ''' + row_value + ''' FROM ''' + table + ''' WHERE ''' + row_value + ''' = ?;'''
            
            t1 = time.perf_counter()
            self.cursor.execute(query, (value,))
            sql_time = time.perf_counter() - t1
            
            values = self.cursor.fetchall()
            print(row_value)
            print(values[0][1])
            
            if (ind == True):
                print(f"Xρόνος ανάκτησης με index: {sql_time:.5f}")
            else:
                print(f"Xρόνος ανάκτησης χωρίς index: {sql_time:.5f}")
                
        except sqlite3.Error as error:
            print("Το query δεν έγινε σφάλμα: ", error)
        
    def drop_indexes(self):
        t1 = time.perf_counter()
        
        try:
            self.cursor.execute('''DROP index IF EXISTS index_pelatis_1;''')
            self.cursor.execute('''DROP index IF EXISTS index_pelatis_2;''')
            self.cursor.execute('''DROP index IF EXISTS index_dieksagogi''')
            self.cursor.execute('''DROP index IF EXISTS index_ekdilosi_1;''')
            self.cursor.execute('''DROP index IF EXISTS index_ekdilosi_2;''')
            self.cursor.execute('''DROP index IF EXISTS index_xoros_1;''')
            self.cursor.execute('''DROP index IF EXISTS index_xoros_2;''')
            self.cursor.execute('''DROP index IF EXISTS index_afora_1;''')
            self.cursor.execute('''DROP index IF EXISTS index_afora_2;''')
            self.cursor.execute('''DROP index IF EXISTS index_thesi;''')

            sql_time = time.perf_counter() - t1
                
            print(f"Επιτυχής διαγραφή των indexes χρόνος: {sql_time:.5f}")
        
        except sqlite3.Error as error:
            print("Οι indexes δεν διαγράφηκαν σφάλμα: ", error)
        
    
if __name__ == "__main__":
    dbfile = "DBKratisis_with_index.db"
    d = DataModel(dbfile) # δημιουργία σύνδεσης στη βάση δεδομένων
    #d.create_indexes()

print()

# index_pelatis_1
print("index_pelatis_1")
value = d.select_random_value('`pelatis`', '`e_mail`')
d.do_queries('`pelatis`', '`id_pelati`', '`e_mail`', value, False, 0)
d.do_queries('`pelatis`', '`id_pelati`', '`e_mail`', value, True, 0)
print()

# index_pelatis_2
print("index_pelatis_2")
value = d.select_random_value('`pelatis`', '`tilefono`')
d.do_queries('`pelatis`', '`id_pelati`', '`tilefono`', value, False, 1)
d.do_queries('`pelatis`', '`id_pelati`', '`tilefono`', value, True, 1)
print()

# index_dieksagogi
print("index_dieksagogi")
value = d.select_random_value('`dieksagogi`', '`date`')
d.do_queries('`dieksagogi`', '`id_ekdilosis`', '`date`', value, False, 2)
d.do_queries('`dieksagogi`', '`id_ekdilosis`', '`date`', value, True, 2)
print()

# index_ekdilosi_1
print("index_ekdilosi_1")
value = d.select_random_value('`ekdilosi`', '`onoma`')
d.do_queries('`ekdilosi`', '`id_ekdilosis`', '`onoma`', value, False, 3)
d.do_queries('`ekdilosi`', '`id_ekdilosis`', '`onoma`', value, True, 3)
print()

# index_ekdilosi_2
print("index_ekdilosi_2")
value = d.select_random_value('`pelatis`', '`e_mail`')
d.do_queries('`pelatis`', '`id_pelati`', '`e_mail`', value, False, 4)
d.do_queries('`pelatis`', '`id_pelati`', '`e_mail`', value, True, 4)
print()

# index_xoros_1
print("index_xoros_1")
value = d.select_random_value('`xoros`', '`e_mail`')
d.do_queries('`xoros`', '`id_xorou`', '`e_mail`', value, False, 5)
d.do_queries('`xoros`', '`id_xorou`', '`e_mail`', value, True, 5)
print()

# index_xoros_2
print("index_xoros_2")
value = d.select_random_value('`xoros`', '`tilefono`')
d.do_queries('`xoros`', '`id_xorou`', '`tilefono`', value, False, 6)
d.do_queries('`xoros`', '`id_xorou`', '`tilefono`', value, True, 6)
print()

# index_afora_1
print("index_afora_1")
value = d.select_random_value('`afora`', '`id_xorou`')
d.do_queries('`afora`', '`id_kratisis`', '`id_xorou`', value, False, 7)
d.do_queries('`afora`', '`id_kratisis`', '`id_xorou`', value, True, 7)
print()

# index_afora_2
print("index_afora_2")
value = d.select_random_value('`afora`', '`date`')
d.do_queries('`afora`', '`id_kratisis`', '`date`', value, False, 8)
d.do_queries('`afora`', '`id_kratisis`', '`date`', value, True, 8)
print()

# index_thesi
print("index_thesi")
value = d.select_random_value('`thesi`', '`id_xorou`')
d.do_queries('`thesi`', '`id_zonis`', '`id_xorou`', value, False, 9)
d.do_queries('`thesi`', '`id_zonis`', '`id_xorou`', value, True, 9)
print()

d.close()