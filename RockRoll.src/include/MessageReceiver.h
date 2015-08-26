/* 
 * File:   MessageReceiver.h
 * Author: sernst
 *
 * Created on December 7, 2011, 8:52 AM
 */

#ifndef MESSAGERECEIVER_H
#define	MESSAGERECEIVER_H

#include <decaf/lang/Thread.h>
#include <decaf/lang/Runnable.h>
//#include <decaf/util/concurrent/Lock.h>
#include <decaf/util/concurrent/CountDownLatch.h>

#include <activemq/core/ActiveMQConnectionFactory.h>

#include <cms/Connection.h>
#include <cms/Session.h>
#include <cms/TextMessage.h>
#include <cms/BytesMessage.h>
#include <cms/MapMessage.h>
#include <cms/ExceptionListener.h>
#include <cms/MessageListener.h>

using namespace cms;
using namespace decaf::lang;
using namespace decaf::util::concurrent;

// define message callback function pointer type
typedef std::string (*messageCallbackFunc)(const std::string&);

class MessageReceiver : public ExceptionListener,
                        public MessageListener,
                        public Runnable {
    
public:
    MessageReceiver(const std::string&  rBrokerURI,
                    const std::string&  rName,
                    bool                bUseTopic       = false,
                    bool                bUseTransaction = false,
                    long                waitMillis      = 30000);
    
    virtual ~MessageReceiver() throw();
    
    void setMessageCallback(messageCallbackFunc messageCallbackFunc) {
        //m_mcfLock.lock();
        m_messageCallbackFunc = messageCallbackFunc;
        //m_mcfLock.unlock();
    }; // setMessageCallbackFunc
    
    void stop() {
        if (m_endThreadLatch.getCount() > 0) {
            m_endThreadLatch.countDown();
        }
    }; // stop
    
    virtual void run();
    virtual void onMessage(const Message*  pMessage) throw();
    virtual void onException(const CMSException& rEx);
    
private:
    void cleanup();
    
    static int g_LibraryInitialized;
    
    CountDownLatch   m_endThreadLatch;
    std::string      m_brokerURI;
    std::string      m_connName;
    Connection*      m_pConn;
    Session*         m_pSession;
    Destination*     m_pDest;
    MessageConsumer* m_pConsumer;
    MessageProducer* m_pReplyProducer;
    long             m_waitMillis;
    bool             m_bUseTopic;
    bool             m_bUseTransaction;
    
    //Lock                m_mcfLock;
    messageCallbackFunc m_messageCallbackFunc;

};

#endif	/* MESSAGERECEIVER_H */

