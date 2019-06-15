package counter;

public class TestEvent {
    private Long id;
    private Long at;
    private Long uid;

    @Override
    public String toString() {
        return "TestEvent{" +
                "id=" + id +
                ", at='" + at + '\'' +
                ", uid=" + uid +
                '}';
    }

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getAt() {
        return at;
    }

    public void setAt(Long at) {
        this.at = at;
    }

    public Long getUid() {
        return uid;
    }

    public void setUid(Long uid) {
        this.uid = uid;
    }

    public TestEvent() {
    }

    public TestEvent(Long id, Long at, Long uid) {
        this.id = id;
        this.at = at;
        this.uid = uid;
    }
}
