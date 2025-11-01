package de.smartdata.lyser.check;

/**
 * Derivated from IOP Core for use of TimeSpanDimension.
 * 
 * @author ffehring
 */
public abstract class Dimension {
    
    private String _name;
    private long _length;
    private boolean _generated;
    private Class _type;

//    private Information _information;
    
    public String getName() {
        return _name;
    }

    public void setName(String _name) {
        this._name = _name;
    }

    public long getLength() {
        return _length;
    }

    public void setLength(long _length) {
        this._length = _length;
    }
    
    public void setGenerated(boolean generated) {
        this._generated = generated;
    }
    
    public boolean isGenerated() {
        return this._generated;
    }
    
    public Class getType() {
        if(this._type==null) {
            this._type = this.getClass();
        }
        return this._type;
    }
    
    public void setType(Class type) {
        this._type = type;
        this._type.cast(this);
    }
    
//    public Information getInformation() {
//        return _information;
//    }
//
//    public void setInformation(Information _information) {
//        this._information = _information;
//    }
    
    public Dimension specialise() {
        return this.getClass().cast(this);
    } 
}
