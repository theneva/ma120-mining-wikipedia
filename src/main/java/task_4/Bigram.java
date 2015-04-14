package task_4;

public class Bigram
{
    private String first;
    private String second;

    public Bigram()
    {
    }

    public Bigram(final String first, final String second)
    {
        this.first = first;
        this.second = second;
    }

    public String getFirst()
    {
        return first;
    }

    public void setFirst(final String first)
    {
        this.first = first;
    }

    public String getSecond()
    {
        return second;
    }

    public void setSecond(final String second)
    {
        this.second = second;
    }

    @Override
    public String toString()
    {
        return "Bigram{" +
                "first='" + first + '\'' +
                ", second='" + second + '\'' +
                '}';
    }
}
