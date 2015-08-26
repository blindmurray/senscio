/*
 * File:   MessageReceiver.cpp
 * Author: sernst
 *
 * Created on December 7, 2011, 8:52 AM
 */

#include "MessageReceiver.h"

#include <activemq/library/ActiveMQCPP.h>

#include <memory>

int MessageReceiver::g_LibraryInitialized = 0;

/**
 *
 * @param rBrokerURI
 * @param rName
 * @param bUseTopic
 * @param bUseTransaction
 * @param waitMillis
 */
MessageReceiver::MessageReceiver(const std::string&  rBrokerURI,
                                 const std::string&  rName,
                                 bool                bUseTopic,
                                 bool                bUseTransaction,
                                 long                waitMillis)
    : m_endThreadLatch(1),
      m_brokerURI(rBrokerURI),
      m_connName(rName),
      m_pConn(NULL),
      m_pSession(NULL),
      m_pDest(NULL),
      m_pConsumer(NULL),
      m_pReplyProducer(NULL),
      m_waitMillis(waitMillis),
      m_bUseTopic(bUseTopic),
      m_bUseTransaction(bUseTransaction),
      //m_mcfLock(m_messageCallbackFunc),
      m_messageCallbackFunc(NULL) {

    if (g_LibraryInitialized == 0) {
        activemq::library::ActiveMQCPP::initializeLibrary();
        g_LibraryInitialized = 1;
    } else {
        ++g_LibraryInitialized;
    }

}

/**
 *
 */
MessageReceiver::~MessageReceiver() throw() {
    cleanup();
}

/**
 *
 */
void
MessageReceiver::run() {

    try {

	std::cout << "Creating factory: " << m_brokerURI << std::endl;

	 // Create a ConnectionFactory
        std::auto_ptr<ConnectionFactory> connectionFactory(ConnectionFactory::createCMSConnectionFactory(m_brokerURI));

	 // Create a Connection
        m_pConn = connectionFactory->createConnection();
        m_pConn->start();
        m_pConn->setExceptionListener(this);

	 // Create a Session
        if (m_bUseTransaction) {
            m_pSession = m_pConn->createSession(Session::SESSION_TRANSACTED);
        } else {
            m_pSession = m_pConn->createSession(Session::AUTO_ACKNOWLEDGE);
        }

	 // Create the destination (Topic or Queue)
        if (m_bUseTopic) {
            m_pDest = m_pSession->createTopic(m_connName);
        } else {
            m_pDest = m_pSession->createQueue(m_connName);
        }

	 /* Create a reply producer to respond to messages from the client.
	  * The destination will be pulled from the JMSReplyTo header field
	  * in the request message. */

        m_pReplyProducer = m_pSession->createProducer(NULL);
        m_pReplyProducer->setDeliveryMode(DeliveryMode::NON_PERSISTENT);

	 // Create a MessageConsumer from the Session to the Topic or Queue
        m_pConsumer = m_pSession->createConsumer(m_pDest);

        m_pConsumer->setMessageListener(this);

        std::cout.flush();
        std::cerr.flush();

	 // Wait for permission to end thread
        m_endThreadLatch.await();

    } catch (CMSException& rEx ) {

        rEx.printStackTrace();

    }

} // MessageReceiver::run

/**
 * Called from the consumer since this class is a registered MessageListener.
 * @param pMessage
 */
void
MessageReceiver::onMessage(const Message* pMessage) throw () {

    static int count = 0;

    try {

        ++count;

        const TextMessage* pTextMessage = dynamic_cast<const TextMessage*>(pMessage);
        std::string        msgText;

        if (pTextMessage != NULL) {
            msgText = pTextMessage->getText();
        } else {
            msgText = "NOT A TEXTMESSAGE!";
        }

        //m_mcfLock.lock();
        if (m_messageCallbackFunc != NULL) {

            std::string responseText(m_messageCallbackFunc(msgText));

            if (!responseText.empty()) {

                Message* pResponse = m_pSession->createTextMessage(responseText);

                pResponse->setCMSCorrelationID(pMessage->getCMSCorrelationID());

                m_pReplyProducer->send(pMessage->getCMSReplyTo(), pResponse);

                delete pResponse;
                pResponse = NULL;

            }

        }
        //m_mcfLock.unlock();

    } catch (CMSException& rEx) {
        std::cerr << "Error in MessageReceiver::onMessage():" << std::endl;
        rEx.printStackTrace();
    }

    // Commit all messages.
    if (m_bUseTransaction) {
        m_pSession->commit();
    }

} // MessageReceiver::onMessage

/**
 * If something bad happens you see it here as this class is also been
 * registered as an ExceptionListener with the connection.
 * @param
 */
void
MessageReceiver::onException(const CMSException& /*rEx AMQCPP_UNUSED*/) {

    std::cout << "CMS Exception occurred.  Shutting down client." << std::endl;
    exit(1);

} // MessageReceiver::onException

/**
 *
 */
void
MessageReceiver::cleanup() {

    //*************************************************
    // Always close destination, consumers and producers before
    // you destroy their sessions and connection.
    //*************************************************

    // Destroy resources.
    try {
        if (m_pDest != NULL) {
            delete m_pDest;
        }
    } catch (CMSException& rEx) {
        rEx.printStackTrace();
    }
    m_pDest = NULL;

    try{
        if (m_pConsumer != NULL) {
            delete m_pConsumer;
        }
    } catch (CMSException& rEx) {
        rEx.printStackTrace();
    }
    m_pConsumer = NULL;

    // Close open resources.
    try {
        if (m_pSession != NULL) {
            m_pSession->close();
        }
        if (m_pConn != NULL) {
            m_pConn->close();
        }
    } catch (CMSException& rEx) {
        rEx.printStackTrace();
    }

    // Now Destroy them
    try {
        if (m_pSession != NULL) {
            delete m_pSession;
        }
    } catch (CMSException& rEx) {
        rEx.printStackTrace();
    }
    m_pSession = NULL;

    try {
        if (m_pConn != NULL) {
            delete m_pConn;
        }
    } catch (CMSException& rEx) {
        rEx.printStackTrace();
    }
    m_pConn = NULL;

    --g_LibraryInitialized;
    if (g_LibraryInitialized <= 0) {
        activemq::library::ActiveMQCPP::shutdownLibrary();
    }

} // MessageReceiver::cleanup

//*** MESSAGERECEIVER.CPP ***

