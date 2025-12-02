public class RequestDto {

    private String id;
    private int a;
    private int b;

    public RequestDto() {
    }

    public RequestDto(String id, int a, int b) {
        this.id = id;
        this.a = a;
        this.b = b;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getA() {
        return a;
    }

    public void setA(int a) {
        this.a = a;
    }

    public int getB() {
        return b;
    }

    public void setB(int b) {
        this.b = b;
    }
}
