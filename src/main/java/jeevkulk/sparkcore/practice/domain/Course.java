package jeevkulk.sparkcore.practice.domain;

import java.io.Serializable;
import java.util.Objects;

public class Course implements Serializable {

    private String code;
    private String name;
    private boolean elective;
    private int semester;
    private String specialization;
    private int credits;

    public Course(String code, String name, boolean elective, int semester, String specialization, int credits) {
        this.code = code;
        this.name = name;
        this.elective = elective;
        this.semester = semester;
        this.specialization = specialization;
        this.credits = credits;
    }

    public String getCode() {
        return code;
    }

    public void setCode(String code) {
        this.code = code;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public boolean isElective() {
        return elective;
    }

    public void setElective(boolean elective) {
        this.elective = elective;
    }

    public int getSemester() {
        return semester;
    }

    public void setSemester(int semester) {
        this.semester = semester;
    }

    public String getSpecialization() {
        return specialization;
    }

    public void setSpecialization(String specialization) {
        this.specialization = specialization;
    }

    public int getCredits() {
        return credits;
    }

    public void setCredits(int credits) {
        this.credits = credits;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Course)) return false;
        Course course = (Course) o;
        return Objects.equals(getCode(), course.getCode());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getCode());
    }
}
