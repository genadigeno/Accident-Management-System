package ams.firerescue.mapper;

import ams.data.model.FireAccidentModel;
import ams.firerescue.jpa.FireAccident;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;
import org.mapstruct.factory.Mappers;

@Mapper
public interface FireRescueMapper {
    FireRescueMapper MAPPER = Mappers.getMapper(FireRescueMapper.class);
    // The event's `id` is a random, non-unique business value — it must NEVER become the
    // sequence-generated primary key (it forces Hibernate onto the merge path, crashes batches
    // with StaleObjectStateException, and silently overwrites rows on id collisions).
    @Mapping(target = "id", ignore = true)
    @Mapping(source = "address", target = "address", qualifiedByName = "charSequenceToString")
    @Mapping(source = "latitude", target = "latitude", qualifiedByName = "charSequenceToString")
    @Mapping(source = "longitude", target = "longitude", qualifiedByName = "charSequenceToString")
    @Mapping(source = "description", target = "description", qualifiedByName = "charSequenceToString")
    FireAccident fireAccidentModelToFireAccident(FireAccidentModel source);

    @Named("charSequenceToString")
    default String charSequenceToString(CharSequence charSequence) {
        return charSequence != null ? charSequence.toString() : null;
    }
}
