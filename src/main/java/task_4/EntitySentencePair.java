package task_4;

public class EntitySentencePair
{
    private String entity;
    private String sentence;

    public EntitySentencePair()
    {
    }

    public EntitySentencePair(final String entity, final String sentence)
    {
        this.entity = entity;
        this.sentence = sentence;
    }

    public String getEntity()
    {
        return entity;
    }

    public void setEntity(final String entity)
    {
        this.entity = entity;
    }

    public String getSentence()
    {
        return sentence;
    }

    public void setSentence(final String sentence)
    {
        this.sentence = sentence;
    }

    @Override
    public String toString()
    {
        return "EntitySentencePair{" +
                "entity='" + entity + '\'' +
                ", sentence='" + sentence + '\'' +
                '}';
    }
}
