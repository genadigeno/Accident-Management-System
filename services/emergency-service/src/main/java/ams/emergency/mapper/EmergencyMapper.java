package ams.emergency.mapper;

import ams.data.model.EmergencyEventModel;
import ams.emergency.jpa.EmergencyAccident;
import org.mapstruct.Mapper;
import org.mapstruct.Mapping;
import org.mapstruct.Named;
import org.mapstruct.factory.Mappers;

@Mapper
public interface EmergencyMapper {
    EmergencyMapper MAPPER = Mappers.getMapper(EmergencyMapper.class);
    @Mapping(source = "address", target = "address", qualifiedByName = "charSequenceToString")
    @Mapping(source = "latitude", target = "latitude", qualifiedByName = "charSequenceToString")
    @Mapping(source = "longitude", target = "longitude", qualifiedByName = "charSequenceToString")
    @Mapping(source = "description", target = "description", qualifiedByName = "charSequenceToString")
    EmergencyAccident emergencyEventModelToEmergencyAccident(EmergencyEventModel source);

    @Named("charSequenceToString")
    default String charSequenceToString(CharSequence charSequence) {
        return charSequence != null ? charSequence.toString() : null;
    }
}
