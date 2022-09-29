
public class Datapoint {
    private double x;
    private double y;

    public Datapoint(String x_string, String y_string) {
        x = Double.parseDouble(x_string);
        y = Double.parseDouble(y_string);
    }

    public Datapoint(Double x_double, Double y_double) {
        x = x_double;
        y = y_double;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public void setXY(double x, double y) {
        this.x = x;
        this.y = y;
    }

    @Override
    public String toString() {
        return "[" + x + "," + y + "]";
    }
}
