package de.smartdata.lyser.check;

import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;

/**
 * Represents an timespan
 * 
 * @author ffehring
 */
public class TimespanDimension extends Dimension {
    
    private LocalDateTime _from;
    private LocalDateTime _until;
    
    public TimespanDimension(LocalDateTime from, LocalDateTime until) {
        this._from = from;
        this._until = until;
    }

    public LocalDateTime getFrom() {
        return _from;
    }

    public void setFrom(LocalDateTime _from) {
        this._from = _from;
    }

    public LocalDateTime getUntil() {
        return _until;
    }

    public void setUntil(LocalDateTime _until) {
        this._until = _until;
    }
    
    public boolean isWithinDateTime(TimespanDimension timespan) {
        return this._from.isAfter(timespan._from) && this._until.isBefore(timespan._until);
    }
    
    public boolean isWithinTime(TimespanDimension timespan) {
        LocalTime thisFromTime = this._from.toLocalTime();
        LocalTime thisUntilTime = this._until.toLocalTime();
        LocalTime queryFromTime = timespan._from.toLocalTime();
        LocalTime queryUntilTime = timespan._until.toLocalTime();
        if((thisFromTime.isAfter(queryFromTime) || thisFromTime.isBefore(queryUntilTime)) &&  (thisUntilTime.isAfter(queryUntilTime) || thisUntilTime.isBefore(queryFromTime))) {
            return true;
        }
        
        return false;
    }
    
    @Override
    public String toString() {
        DateTimeFormatter formatter = DateTimeFormatter.ISO_LOCAL_DATE_TIME;
        return formatter.format(_from) + " - " + this._until;
    }
}
