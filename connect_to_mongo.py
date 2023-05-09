# Import Libraries
from pymongo import MongoClient
import ssl

# Create connection to mongo DB with client object
def connection(hostname):
    try:
        MONGO_HOST = hostname 
        MONGO_PKEY='mongodb-ca.pem'
        MONGO_PRIVATEKEY='mongodb-cert-key.pem'
        port_user=27017
        client = MongoClient(MONGO_HOST,
                         port=port_user,
                         ssl=True,
                         tlsCertificateKeyFile=MONGO_PRIVATEKEY,
                         tlsCAFile=MONGO_PKEY,
                         tlsAllowInvalidCertificates=True,
                         connect=True)
    except:
        try:
            MONGO_HOST = hostname
            MONGO_PKEY='mongodb-ca.pem'
            MONGO_PRIVATEKEY='mongodb-cert-key.pem'
            port_user=27017
            client = MongoClient(MONGO_HOST,
                                 port=port_user,
                                 ssl=True,
                                 ssl_certfile=MONGO_PRIVATEKEY,
                                 ssl_ca_certs=MONGO_PKEY,
                                 ssl_cert_reqs=ssl.CERT_NONE,
                                 connect=True)
        except:
            pass
    return(client)

if __name__ == '__main__':
    test='100.101.243.4'
    client = connection(test)
    print(client)

