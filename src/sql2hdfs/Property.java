/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package sql2hdfs;

/**
 *
 * @author tarun
 */
public class Property {
    private String driver;
    private String url;
    private String user;
    private String pass;

    public boolean equals(Property obj) {
        if(getUrl().equals(obj.getUrl())){
            if(getUser().equals(obj.getUrl())){
                if(getPass().equals(obj.getPass())){
                    return true;
                }
                else
                    return false;
            }
            else
                return false;
        }
        return false;
    }
    
    public String getDriver() {
        return driver;
    }

    public void setDriver(String driver) {
        this.driver = driver;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPass() {
        return pass;
    }

    public void setPass(String pass) {
        this.pass = pass;
    }
}
