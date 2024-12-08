import java.io.Serializable;
import java.util.Objects;

public class Area implements Serializable {
    public String name;
    public Long parent_id;

    public Area() {}


    @Override
    public boolean equals(Object o) {
        if (o == null || getClass() != o.getClass()) return false;
        Area area = (Area) o;
        return Objects.equals(name, area.name) && Objects.equals(parent_id, area.parent_id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(name, parent_id);
    }
}
