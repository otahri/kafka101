package eventPersons;

import kafka101.mydev.Event;

import java.util.Iterator;

final class PersonIterator implements Iterator<Event> {

    private final PersonGenerator personGenerator;

    public PersonIterator(PersonGenerator personGenerator) {
        this.personGenerator = personGenerator;
    }

    @Override
    public boolean hasNext() {
        return true;
    }

    @Override
    public Event next() {
        try {
            Thread.sleep(500);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return personGenerator.generate();
    }
}
