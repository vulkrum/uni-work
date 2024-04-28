-- Q1: ...

create or replace view Q1(unswid, name)
as
select people.unswid, people.name
from people
join course_enrolments
on people.id = course_enrolments.student
group by people.unswid, people.name
having count(*) > 65;

-- Q2: ...

create or replace view Q2(nstudents, nstaff, nboth)
as
select
    (select count(*) as nstudents from students
        left join staff on students.id = staff.id where staff.id is null),
    (select count(*) as nstaff from staff
        left join students on staff.id = students.id where students.id is null),
    (select count(*) as nboth from students
        inner join staff on students.id = staff.id);

-- Q3: ...

create or replace view Q3(name, ncourses)
as
select people.name, count(*) as ncourses
from people
join course_staff
on people.id = course_staff.staff
join (select * from staff_roles where staff_roles.name = 'Course Convenor') as sr
on course_staff.role = sr.id
group by people.name
order by ncourses desc
limit 1;

-- Q4: ...

create or replace view Q4a(id)
as
select people.unswid as id
from people, program_enrolments, semesters, programs
where people.id = program_enrolments.student and program_enrolments.semester = semesters.id 
    and semesters.year = 2005 and semesters.term = 'S2' and programs.code = '3978' 
    and program_enrolments.program = programs.id;

create or replace view Q4b(id)
as
select people.unswid as id
from people, program_enrolments, stream_enrolments, semesters, streams
where people.id = program_enrolments.student and program_enrolments.semester = semesters.id
    and semesters.year = 2005 and semesters.term = 'S2' and stream_enrolments.partof = program_enrolments.id
    and stream_enrolments.stream = streams.id and streams.code = 'SENGA1';

create or replace view Q4c(id)
as
select people.unswid as id
from people, program_enrolments, programs, orgunits, semesters
where people.id = program_enrolments.student and program_enrolments.semester = semesters.id
    and semesters.year = 2005 and semesters.term = 'S2' and program_enrolments.program = programs.id
    and programs.offeredby = orgunits.id and orgunits.longname = 'School of Computer Science and Engineering';

-- Q5: ...

create or replace view Q5(name)
as
select orgunits.name
from orgunits
join (select facultyof(committees.id) as id, count(*)
        from (select orgunits.id
                from orgunits
                join (select * from orgunit_types where orgunit_types.name = 'Committee') as ou_t
                on orgunits.utype = ou_t.id) as committees
        where facultyof(committees.id) is not null
        group by facultyof(committees.id)
        order by count(*) desc
        limit 1) as result
on orgunits.id = result.id;

-- Q6: ...

create or replace function Q6(integer) returns text
as
$$
select name
from people
where people.id = $1 or people.unswid = $1;
$$ language sql
;

-- Q7: ...

create or replace function Q7(text)
	returns table (course text, year integer, term text, convenor text)
as $$
select subs.code::text, semesters.year, semesters.term::text, people.name::text
from course_staff
join (select * from staff_roles where staff_roles.name = 'Course Convenor') as sr
on course_staff.role = sr.id
join courses
on course_staff.course = courses.id
join (select * from subjects where subjects.code = $1) as subs
on courses.subject = subs.id
join semesters
on courses.semester = semesters.id
join people
on course_staff.staff = people.id
$$ language sql
;

-- Q8: ...

create or replace function Q8(integer)
	returns setof NewTranscriptRecord
as $$
declare
    rec TranscriptRecord;
    new NewTranscriptRecord;
    progcode char(4);
begin
for rec in select * from transcript($1)
    loop
        select programs.code into progcode
        from people
        join program_enrolments
        on program_enrolments.student = people.id
        join programs
        on programs.id = program_enrolments.program
        join course_enrolments
        on course_enrolments.student = people.id
        join courses
        on courses.id = course_enrolments.course
        join subjects
        on courses.subject = subjects.id
        join semesters
        on program_enrolments.semester = semesters.id and courses.semester = semesters.id
        where people.unswid = $1 and substr(subjects.name,1,20) = rec.name
        and substr(semesters.year::text,3,2)||lower(semesters.term) = rec.term;

        new =  (rec.code, rec.term, progcode, rec.name, rec.mark, rec.grade, rec.uoc);
        return next new;
    end loop;
end;
$$ language plpgsql
;

-- Q9: ...

create or replace function Q9(integer)
	returns setof AcObjRecord
as $$
declare
    rec AcObjRecord;
    objType text;
    tableType text;
    groupDef text;
    def text;
    patterns text[];
    expr text;
    temp text;
    code text;
begin
    select acad_object_groups.gtype into objType
    from acad_object_groups where acad_object_groups.id = $1;

    select acad_object_groups.gdefby into groupDef
    from acad_object_groups where acad_object_groups.id = $1;

    tableType = concat(objType, 's');

    if(groupDef != 'pattern') then
        return;
    else
        select acad_object_groups.definition into def
        from acad_object_groups where acad_object_groups.id = $1;

        if((def ~ '{') or (def ~ '=')) then
            return;
        else
            patterns = regexp_split_to_array(def, ',');
            foreach expr in array patterns
            loop
                if((expr !~ 'FREE') or (expr !~ 'GEN') or (expr !~ 'ZGEN')) then
                    temp = replace(expr, '#', '.');
                    expr = temp;
                else
                    rec = (objType, expr);
                    return next rec;

                end if;

                for code in execute 'select code from ' || tableType::regclass
                loop
                    if(code ~ expr) then
                        rec = (objType, code);
                        return next rec;
                    else
                        continue;
                    end if;

                end loop;

            end loop;

        end if;

    end if;
end;
$$ language plpgsql
;

