public class ResponseDto {

    private String id;
    private int result;

    public ResponseDto() {
    }

    public ResponseDto(String id, int result) {
        this.id = id;
        this.result = result;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public int getResult() {
        return result;
    }

    public void setResult(int result) {
        this.result = result;
    }
}
