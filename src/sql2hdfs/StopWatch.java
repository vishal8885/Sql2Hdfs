package sql2hdfs;

public class StopWatch {

    private long duration = 0;
    private long start = 0;
    
    private boolean active = false;

    public void start() {
        this.start = System.currentTimeMillis();
        this.active = true;
    }
    

    public void stop() {
        if (this.active) this.duration += (System.currentTimeMillis() - this.start);
        this.active = false;
    }


    public void reset() {
        this.duration = 0;
        this.start = 0;
        this.active = false;
    }
    
    public void restart() {
        this.reset();
        this.start();
    }
    
    public long getMilliseconds() {
        if (this.active) return this.duration + System.currentTimeMillis() - this.start;
        else return this.duration;
    }
    
    public double getSeconds() {
        if (this.active) return (this.duration + System.currentTimeMillis() - this.start) / 1000d;
        else return this.duration / 1000d;
    }
    
    public double getMinutes() {
        if (this.active) return (this.duration + System.currentTimeMillis() - this.start) / 60000d;
        else return this.duration / 60000d;
    }

    public boolean isGoing() {
        return this.active;
    }
}