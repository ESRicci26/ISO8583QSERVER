//package org.jpos.q2.iso;

import org.jdom2.Element;
import org.jpos.core.ConfigurationException;
import org.jpos.iso.ISOChannel;
import org.jpos.iso.ISOException;
import org.jpos.iso.ISOMsg;
import org.jpos.iso.ISORequestListener;
import org.jpos.iso.ISOServer;
import org.jpos.iso.ISOServerEventListener;
import org.jpos.iso.ISOServerSocketFactory;
import org.jpos.iso.ISOSource;
import org.jpos.iso.ServerChannel;
import org.jpos.q2.QBeanSupport;
import org.jpos.q2.QFactory;
import org.jpos.space.LocalSpace;
import org.jpos.space.Space;
import org.jpos.space.SpaceFactory;
import org.jpos.space.SpaceListener;
import org.jpos.util.LogSource;
import org.jpos.util.NameRegistrar;
import org.jpos.util.ThreadPool;

import java.util.Iterator;
import java.util.StringTokenizer;
import org.jpos.q2.iso.ChannelAdaptor;
import org.jpos.q2.iso.QServerMBean;


@SuppressWarnings("unchecked")
public class QServer extends QBeanSupport implements QServerMBean, SpaceListener, ISORequestListener {
    private int port = 0;
    private int maxSessions = 100;
    private int minSessions = 1;
    private String channelString, packagerString, socketFactoryString;
    private ISOChannel channel = null;
    private ISOServer server;
    protected LocalSpace sp;
    private String inQueue;
    private String outQueue;
    private String sendMethod;

    public QServer() {
        super();
    }

    @Override
    public void initService() throws ConfigurationException {
        Element e = getPersist();
        sp = grabSpace(e.getChild("space"));
    }

    private void newChannel() throws ConfigurationException {
        Element persist = getPersist();
        Element e = persist.getChild("channel");
        if (e == null) {
            throw new ConfigurationException("channel elemento faltando");
        }

        ChannelAdaptor adaptor = new ChannelAdaptor();
        channel = adaptor.newChannel(e, getFactory());
    }

    private void initServer() throws ConfigurationException {
        if (port == 0) {
            throw new ConfigurationException("Valor da porta nao definido");
        }
        newChannel();
        if (channel == null) {
            throw new ConfigurationException("ISO Channel nulo");
        }

        if (!(channel instanceof ServerChannel)) {
            throw new ConfigurationException(channelString + "NAO implementa ServerChannel");
        }

        ThreadPool pool = null;
        pool = new ThreadPool(minSessions, maxSessions, getName() + "-ThreadPool");
        pool.setLogger(log.getLogger(), getName() + ".pool");

        server = new ISOServer(port, (ServerChannel) channel, pool);
        server.setLogger(log.getLogger(), getName() + ".server");
        server.setName(getName());
        if (socketFactoryString != null) {
            ISOServerSocketFactory sFac = (ISOServerSocketFactory) getFactory().newInstance(socketFactoryString);
            if (sFac != null && sFac instanceof LogSource) {
                ((LogSource) sFac).setLogger(log.getLogger(), getName() + ".socket-factory");
            }
            server.setSocketFactory(sFac);
        }
        getFactory().setConfiguration(server, getPersist());
        addServerSocketFactory();
        addListeners();// ISORequestListener
        addISOServerConnectionListeners();
        NameRegistrar.register(getName(), this);
        new Thread(server).start();
    }

    private void initIn() {
        Element persist = getPersist();
        inQueue = persist.getChildText("in");
        if (inQueue != null) {

            sp.addListener(inQueue, this);
        }
    }

    private void initOut() {
        Element persist = getPersist();
        outQueue = persist.getChildText("out");
        if (outQueue != null) {
            server.addISORequestListener(this);
        }
    }

    @Override
    public void startService() {
        try {
            initServer();
            initIn();
            initOut();
            initWhoToSendTo();
        } catch (Exception e) {
            getLog().warn("erro ao iniciar o servico", e);
        }
    }

    private void initWhoToSendTo() {

        Element persist = getPersist();
        sendMethod = persist.getChildText("send-request");
        if (sendMethod == null) {
            sendMethod = "LAST";
        }

    }

    @Override
    public void stopService() {
        if (server != null) {
            server.shutdown();
            sp.removeListener(inQueue, this);
        }
    }

    @Override
    public void destroyService() {
        NameRegistrar.unregister(getName());
        NameRegistrar.unregister("server." + getName());
    }

    @Override
    public synchronized void setPort(int port) {
        this.port = port;
        setAttr(getAttrs(), "port", port);
        setModified(true);
    }

    @Override
    public int getPort() {
        return port;
    }

    @Override
    public synchronized void setPackager(String packager) {
        packagerString = packager;
        setAttr(getAttrs(), "packager", packagerString);
        setModified(true);
    }

    @Override
    public String getPackager() {
        return packagerString;
    }

    @Override
    public synchronized void setChannel(String channel) {
        channelString = channel;
        setAttr(getAttrs(), "channel", channelString);
        setModified(true);
    }

    @Override
    public String getChannel() {
        return channelString;
    }

    @Override
    public synchronized void setMaxSessions(int maxSessions) {
        this.maxSessions = maxSessions;
        setAttr(getAttrs(), "maxSessions", maxSessions);
        setModified(true);
    }

    @Override
    public int getMaxSessions() {
        return maxSessions;
    }

    @Override
    public synchronized void setMinSessions(int minSessions) {
        this.minSessions = minSessions;
        setAttr(getAttrs(), "minSessions", minSessions);
        setModified(true);
    }

    @Override
    public int getMinSessions() {
        return minSessions;
    }

    @Override
    public synchronized void setSocketFactory(String sFactory) {
        socketFactoryString = sFactory;
        setAttr(getAttrs(), "socketFactory", socketFactoryString);
        setModified(true);
    }

    @Override
    public String getSocketFactory() {
        return socketFactoryString;
    }

    @Override
    public String getISOChannelNames() {
        return server.getISOChannelNames();
    }

    @Override
    public String getCountersAsString() {
        return server.getCountersAsString();
    }

    @Override
    public String getCountersAsString(String isoChannelName) {
        return server.getCountersAsString(isoChannelName);
    }

    private void addServerSocketFactory() throws ConfigurationException {
        QFactory factory = getFactory();
        Element persist = getPersist();

        Element serverSocketFactoryElement = persist.getChild("server-socket-factory");

        if (serverSocketFactoryElement != null) {
            ISOServerSocketFactory serverSocketFactory = (ISOServerSocketFactory) factory
                    .newInstance(serverSocketFactoryElement.getAttributeValue("class"));
            factory.setLogger(serverSocketFactory, serverSocketFactoryElement);
            factory.setConfiguration(serverSocketFactory, serverSocketFactoryElement);
            server.setSocketFactory(serverSocketFactory);
        }

    }

    private void addListeners() throws ConfigurationException {
        QFactory factory = getFactory();
        Iterator iter = getPersist().getChildren("request-listener").iterator();
        while (iter.hasNext()) {
            Element l = (Element) iter.next();
            ISORequestListener listener = (ISORequestListener) factory.newInstance(l.getAttributeValue("class"));
            factory.setLogger(listener, l);
            factory.setConfiguration(listener, l);
            server.addISORequestListener(listener);
        }
    }

    private void addISOServerConnectionListeners() throws ConfigurationException {

        QFactory factory = getFactory();
        Iterator iter = getPersist().getChildren("connection-listener").iterator();
        while (iter.hasNext()) {
            Element l = (Element) iter.next();
            ISOServerEventListener listener = (ISOServerEventListener) factory
                    .newInstance(l.getAttributeValue("class"));
            factory.setLogger(listener, l);
            factory.setConfiguration(listener, l);
            server.addServerEventListener(listener);
        }
    }

    private LocalSpace grabSpace(Element e) throws ConfigurationException {
        String uri = e != null ? e.getText() : "";
        Space sp = SpaceFactory.getSpace(uri);
        if (sp instanceof LocalSpace) {
            return (LocalSpace) sp;
        }
        throw new ConfigurationException("Invalid space " + uri);
    }


    @Override
    public void notify(Object key, Object value) {
        Object obj = sp.inp(key);
        if (obj instanceof ISOMsg) {
            ISOMsg m = (ISOMsg) obj;
            if ("LAST".equals(sendMethod)) {
                try {
                    ISOChannel c = server.getLastConnectedISOChannel();
                    if (c == null) {
                        throw new ISOException("O servidor nao tem conexoes ativas");
                    }
                    if (!c.isConnected()) {
                        throw new ISOException("Client Disconectado");
                    }
                    c.send(m);
                } catch (Exception e) {
                    getLog().warn("notify", e);
                }
            } else if ("ALL".equals(sendMethod)) {
                String channelNames = getISOChannelNames();
                if (channelNames != null) {
                    StringTokenizer tok = new StringTokenizer(channelNames, " ");
                    while (tok.hasMoreTokens()) {
                        try {
                            ISOChannel c = server.getISOChannel(tok.nextToken());
                            if (c == null) {
                                throw new ISOException("O servidor NAO tem conexoes ativas");
                            }
                            if (!c.isConnected()) {
                                throw new ISOException("Client Disconectado");
                            }
                            c.send(m);
                        } catch (Exception e) {
                            getLog().warn("notify", e);
                        }
                    }
                }
            }
        }
    }


    @Override
    public boolean process(ISOSource source, ISOMsg m) {
        sp.out(outQueue, m);
        return true;
    }

}

