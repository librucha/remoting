package hudson.remoting;

import java.io.EOFException;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * {@link CommandTransport} that implements the read operation in a synchronous fashion.
 * 
 * <p>
 * This class uses a thread to pump commands and pass them to {@link CommandReceiver}.
 *     
 * @author Kohsuke Kawaguchi
 */
public abstract class SynchronousCommandTransport extends CommandTransport {
    protected Channel channel;
    private ReaderThread readerThread;

    /**
     * Called by {@link Channel} to read the next command to arrive from the stream.
     */
    abstract Command read() throws IOException, ClassNotFoundException, InterruptedException;

    @Override
    public void setup(Channel channel, CommandReceiver receiver) {
        this.channel = channel;
        ReaderThread readerThread = new ReaderThread(receiver);
        this.readerThread = readerThread;
        readerThread.start();
    }

    public final class ReaderThread extends Thread {

        private volatile boolean paused = false;

        private int commandsReceived = 0;
        private int commandsExecuted = 0;
        private final CommandReceiver receiver;

        public ReaderThread(CommandReceiver receiver) {
            super("Channel reader thread: "+channel.getName());
            this.receiver = receiver;
        }

        @Override
        public void run() {
            final String name = channel.getName();
            final String simpleName = (name != null && !name.isEmpty()) ? name.substring(0, name.contains("[") ? name.indexOf("[") : name.length()).trim() : "";
            final String pid = (name != null && !name.isEmpty() && name.contains(",") && name.contains("]")) ? name.substring(name.lastIndexOf(",") + 1, name.lastIndexOf("]")).trim() : "N/A";
            try {
                while(!channel.isInClosed()) {
                  synchronized (this) {
                    while (paused) {
                      LOGGER.log(Level.INFO, simpleName + " pid=" + pid + " paused");
                      wait();
                      }
                  }
                    Command cmd = null;
                    try {
                        cmd = read();
                    } catch (EOFException e) {
                        IOException ioe = new IOException("Unexpected termination of the channel");
                        ioe.initCause(e);
                        throw ioe;
                    } catch (ClassNotFoundException e) {
                        LOGGER.log(Level.SEVERE, "Unable to read a command (channel " + name + ")",e);
                        continue;
                    } finally {
                        commandsReceived++;
                    }

                    receiver.handle(cmd);
                    commandsExecuted++;
                }
                closeRead();
            } catch (InterruptedException e) {
                LOGGER.log(Level.SEVERE, "I/O error in channel "+name,e);
                channel.terminate((InterruptedIOException) new InterruptedIOException().initCause(e));
            } catch (IOException e) {
                LOGGER.log(Level.SEVERE, "I/O error in channel "+name,e);
                channel.terminate(e);
            } catch (RuntimeException e) {
                LOGGER.log(Level.SEVERE, "Unexpected error in channel "+name,e);
                channel.terminate((IOException) new IOException("Unexpected reader termination").initCause(e));
                throw e;
            } catch (Error e) {
                LOGGER.log(Level.SEVERE, "Unexpected error in channel "+name,e);
                channel.terminate((IOException) new IOException("Unexpected reader termination").initCause(e));
                throw e;
            } finally {
                channel.pipeWriter.shutdown();
            }
        }
        
        public boolean isPaused(){
          return paused;
        }
        
        public void pause(){
          paused = true;
        }
        
        public void cont(){
          final String name = channel.getName();
          final String simpleName = (name != null && !name.isEmpty()) ? name.substring(0, name.contains("[") ? name.indexOf("[") : name.length()).trim() : "";
          final String pid = (name != null && !name.isEmpty() && name.contains(",") && name.contains("]")) ? name.substring(name.lastIndexOf(",") + 1, name.lastIndexOf("]")).trim() : "N/A";
          synchronized (this) {
           paused = false;
           LOGGER.log(Level.INFO, simpleName + " pid=" + pid + " continue");
           notify();
          }
        }
    }
    
    public ReaderThread getReaderThread() {
      return readerThread;
    }
    
    private static final Logger LOGGER = Logger.getLogger(SynchronousCommandTransport.class.getName());
}
