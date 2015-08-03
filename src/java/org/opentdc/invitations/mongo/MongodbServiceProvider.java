/**
 * The MIT License (MIT)
 *
 * Copyright (c) 2015 Arbalo AG
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package org.opentdc.invitations.mongo;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.logging.Logger;

import javax.servlet.ServletContext;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.opentdc.mongo.AbstractMongodbServiceProvider;
import org.opentdc.events.EventModel;
import org.opentdc.invitations.InvitationModel;
import org.opentdc.invitations.InvitationState;
import org.opentdc.invitations.SalutationType;
import org.opentdc.invitations.ServiceProvider;
import org.opentdc.service.exception.DuplicateException;
import org.opentdc.service.exception.InternalServerErrorException;
import org.opentdc.service.exception.NotFoundException;
import org.opentdc.service.exception.ValidationException;
import org.opentdc.util.EmailSender;
import org.opentdc.util.FreeMarkerConfig;
import org.opentdc.util.PrettyPrinter;

import freemarker.template.Template;

/**
 * A MongoDB-based implementation of the invitations service.
 * @author Bruno Kaiser
 *
 */
public class MongodbServiceProvider 
	extends AbstractMongodbServiceProvider<InvitationModel> 
	implements ServiceProvider 
{
	private static final Logger logger = Logger.getLogger(MongodbServiceProvider.class.getName());
	private EmailSender emailSender = null;
	private static final String SUBJECT = "Einladung zum Arbalo Launch Event";
	private ServletContext context = null;

	/**
	 * Constructor.
	 * @param context the servlet context.
	 * @param prefix the simple class name of the service provider; this is also used as the collection name.
	 */
	public MongodbServiceProvider(
		ServletContext context, 
		String prefix) 
	{
		super(context);
		this.context = context;
		connect();
		collectionName = prefix;
		getCollection(collectionName);
		new FreeMarkerConfig(context);
		emailSender = new EmailSender(context);
		logger.info("MongodbServiceProvider(context, " + prefix + ") -> OK");
	}
	
	private Document convert(InvitationModel model, boolean withId) 
	{
		Document _doc = new Document("firstName", model.getFirstName())
			.append("lastName", model.getLastName())
			.append("email", model.getEmail())
			.append("comment", model.getComment())
			.append("contact",  model.getContact())
			.append("salutation", model.getSalutation().toString())
			.append("invitationState", model.getInvitationState().toString())
			.append("createdAt", model.getCreatedAt())
			.append("createdBy", model.getCreatedBy())
			.append("modifiedAt", model.getModifiedAt())
			.append("modifiedBy", model.getModifiedBy());
		// for backwards compatibility reason
		if (model.getInternalComment() != null) {
			_doc.append("internalComment", model.getInternalComment());
		}
		if (withId == true) {
			_doc.append("_id", new ObjectId(model.getId()));
		}
		return _doc;
	}
	
	private InvitationModel convert(Document doc)
	{
		InvitationModel _model = new InvitationModel();
		_model.setId(doc.getObjectId("_id").toString());
		_model.setFirstName(doc.getString("firstName"));
		_model.setLastName(doc.getString("lastName"));
		_model.setEmail(doc.getString("email"));
		_model.setComment(doc.getString("comment"));
		_model.setInternalComment(doc.getString("internalComment"));
		_model.setContact(doc.getString("contact"));
		_model.setSalutation(SalutationType.valueOf(doc.getString("salutation")));
		_model.setInvitationState(InvitationState.valueOf(doc.getString("invitationState")));
		_model.setCreatedAt(doc.getDate("createdAt"));
		_model.setCreatedBy(doc.getString("createdBy"));
		_model.setModifiedAt(doc.getDate("modifiedAt"));
		_model.setModifiedBy(doc.getString("modifiedBy"));
		return _model;
	}

	/* (non-Javadoc)
	 * @see org.opentdc.invitations.ServiceProvider#list(java.lang.String, java.lang.String, long, long)
	 */
	@Override
	public ArrayList<InvitationModel> list(
		String queryType,
		String query,
		int position,
		int size) {
		List<Document> _docs = list(position, size);
		ArrayList<InvitationModel> _selection = new ArrayList<InvitationModel>();
		for (Document doc : _docs) {
			_selection.add(convert(doc));
		}
		logger.info("list(<" + query + ">, <" + queryType + 
				">, <" + position + ">, <" + size + ">) -> " + _selection.size() + " invitations.");
		return _selection;
	}

	/* (non-Javadoc)
	 * @see org.opentdc.invitations.ServiceProvider#create(org.opentdc.invitations.InvitationModel)
	 */
	@Override
	public InvitationModel create(
		InvitationModel model) 
	throws DuplicateException, ValidationException {
		logger.info("create(" + PrettyPrinter.prettyPrintAsJSON(model) + ")");
		if (model.getId() == null || model.getId().isEmpty()) {
			model.setId(new ObjectId().toString());			
		}
		else {		// id set
			if (convert(readOne(model.getId())) == null) {
				throw new ValidationException("invitation <" + model.getId() + "> contains an id generated on the client.");	
			} else { 		// object with same id exists already
				throw new DuplicateException("invitation <" + model.getId() + "> exists already.");
			}
		}
		// enforce mandatory fields
		if (model.getFirstName() == null || model.getFirstName().length() == 0) {
			throw new ValidationException("invitation must contain a valid firstName.");
		}
		if (model.getLastName() == null || model.getLastName().length() == 0) {
			throw new ValidationException("invitation must contain a valid lastName.");
		}
		if (model.getEmail() == null || model.getEmail().length() == 0) {
			throw new ValidationException("invitation must contain a valid email address.");
		}
		// set default values
		if (model.getInvitationState() == null) {
			model.setInvitationState(InvitationState.INITIAL);
		}
		if (model.getSalutation() == null) {
			model.setSalutation(SalutationType.DU_M);
		}
		// set modification / creation values
		Date _date = new Date();
		model.setCreatedAt(_date);
		model.setCreatedBy(getPrincipal());
		model.setModifiedAt(_date);
		model.setModifiedBy(getPrincipal());
		
		create(convert(model, true));
		logger.info("create(" + PrettyPrinter.prettyPrintAsJSON(model) + ")");
		return model;
	}

	/* (non-Javadoc)
	 * @see org.opentdc.invitations.ServiceProvider#read(java.lang.String)
	 */
	@Override
	public InvitationModel read(
		String id) 
	throws NotFoundException {
		InvitationModel _invitation = convert(readOne(id));
		if (_invitation == null) {
			throw new NotFoundException("no invitation with ID <" + id
					+ "> was found.");
		}
		logger.info("read(" + id + ") -> " + PrettyPrinter.prettyPrintAsJSON(_invitation));
		return _invitation;
	}

	/* (non-Javadoc)
	 * @see org.opentdc.invitations.ServiceProvider#update(java.lang.String, org.opentdc.invitations.InvitationModel)
	 */
	@Override
	public InvitationModel update(
		String id, 
		InvitationModel invitation
	) throws NotFoundException, ValidationException {
		InvitationModel _invitation = read(id);
		if (! _invitation.getCreatedAt().equals(invitation.getCreatedAt())) {
			logger.warning("invitation <" + id + ">: ignoring createdAt value <" + invitation.getCreatedAt().toString() + 
					"> because it was set on the client.");
		}
		if (! _invitation.getCreatedBy().equalsIgnoreCase(invitation.getCreatedBy())) {
			logger.warning("invitation <" + id + ">: ignoring createdBy value <" + invitation.getCreatedBy() +
					"> because it was set on the client.");
		}
		if (invitation.getFirstName() == null || invitation.getFirstName().length() == 0) {
			throw new ValidationException("invitation <" + id + 
					"> must contain a valid firstName.");
		}
		if (invitation.getLastName() == null || invitation.getLastName().length() == 0) {
			throw new ValidationException("invitation <" + id + 
					"> must contain a valid lastName.");
		}
		if (invitation.getEmail() == null || invitation.getEmail().length() == 0) {
			throw new ValidationException("invitation <" + id + 
					"> must contain a valid email address.");
		}
		if (invitation.getInvitationState() == null) {
			invitation.setInvitationState(InvitationState.INITIAL);
		}
		if (invitation.getSalutation() == null) {
			invitation.setSalutation(SalutationType.DU_M);
		}
		_invitation.setFirstName(invitation.getFirstName());
		_invitation.setLastName(invitation.getLastName());
		_invitation.setEmail(invitation.getEmail());
		_invitation.setContact(invitation.getContact());
		_invitation.setSalutation(invitation.getSalutation());
		_invitation.setInvitationState(invitation.getInvitationState());
		_invitation.setComment(invitation.getComment());
		_invitation.setInternalComment(invitation.getInternalComment());
		_invitation.setModifiedAt(new Date());
		_invitation.setModifiedBy(getPrincipal());
		update(id, convert(_invitation, true));
		logger.info("update(" + id + ") -> " + PrettyPrinter.prettyPrintAsJSON(_invitation));
		return _invitation;
	}

	/* (non-Javadoc)
	 * @see org.opentdc.invitations.ServiceProvider#delete(java.lang.String)
	 */
	@Override
	public void delete(
		String id) 
	throws NotFoundException, InternalServerErrorException {
		read(id);
		deleteOne(id);
		logger.info("delete(" + id + ") -> OK");
	}

	/* (non-Javadoc)
	 * @see org.opentdc.invitations.ServiceProvider#getMessage(java.lang.String)
	 */
	@Override
	public String getMessage(String id) throws NotFoundException,
			InternalServerErrorException {
		logger.info("getMessage(" + id + ")");
		InvitationModel _model = read(id);
		
		// create the FreeMarker data model
        Map<String, Object> _root = new HashMap<String, Object>();    
        _root.put("invitation", _model);
        
        // Merge data model with template   
        String _msg = FreeMarkerConfig.getProcessedTemplate(
        		_root, 
        		getTemplate(_model.getSalutation(), _model.getContact()));
		logger.info("getMessage(" + id + ") -> " + _msg);
		return _msg;
	}
	
	/**
	 * Retrieve the email address of the contact.
	 * @param contactName the name of the contact
	 * @return the corresponding email address
	 */
	private String getEmailAddress(String contactName) {
		logger.info("getEmailAddress(" + contactName + ")");
		String _emailAddress = null;
		if (contactName == null || contactName.isEmpty()) {
			contactName = "arbalo";
		}
	       if (contactName.equalsIgnoreCase("bruno")) {
	        	_emailAddress = "bruno.kaiser@arbalo.ch";
	        } else if (contactName.equalsIgnoreCase("thomas")) {
	        	_emailAddress = "thomas.huber@arbalo.ch";
	        } else if (contactName.equalsIgnoreCase("peter")) {
	        	_emailAddress = "peter.windemann@arbalo.ch";
	        } else if (contactName.equalsIgnoreCase("marc")) {
	        	_emailAddress = "marc.hofer@arbalo.ch";
	        } else if (contactName.equalsIgnoreCase("werner")) {
	        	_emailAddress = "werner.froidevaux@arbalo.ch";        	
	        } else {
	        	_emailAddress = "info@arbalo.ch";        	        	
	        }
	        logger.info("getEmailAddress(" + contactName + ") -> " + _emailAddress);
	        return _emailAddress;	
	}
	
	/**
	 * @param salutation
	 * @return
	 */
	private Template getTemplate(
			SalutationType salutation, String contactName) {
		String _templateName = null;
		if (contactName == null || contactName.isEmpty()) {
			contactName = "arbalo";
		}
		switch (salutation) {
		case HERR: _templateName = "emailHerr_" + contactName + ".ftl"; break;
		case FRAU: _templateName = "emailFrau_" + contactName + ".ftl"; break;
		case DU_F: _templateName = "emailDuf_" + contactName + ".ftl";  break;
		case DU_M: _templateName = "emailDum_" + contactName + ".ftl";  break;
		}
		return FreeMarkerConfig.getTemplateByName(_templateName);
	}

	/* (non-Javadoc)
	 * @see org.opentdc.invitations.ServiceProvider#sendMessage(java.lang.String)
	 */
	@Override
	public void sendMessage(
			String id) 
			throws NotFoundException, InternalServerErrorException {
		logger.info("sendMessage(" + id + ")");
		InvitationModel _model = read(id);

		emailSender.sendMessage(
			_model.getEmail(),
			getEmailAddress(_model.getContact()),
			SUBJECT,
			getMessage(id));
		logger.info("sent email message to " + _model.getEmail());
		_model.setId(null);
		_model.setInvitationState(InvitationState.SENT);
		update(id, _model);
	}

	/* (non-Javadoc)
	 * @see org.opentdc.invitations.ServiceProvider#sendAllMessages()
	 */
	@Override
	public void sendAllMessages() 
			throws InternalServerErrorException {
		logger.info("sendAllMessages()");
		InvitationModel _model = null;
		String _id = null;
		for (Document doc : list(0, 200)) {
			_model = convert(doc);
			_id = _model.getId();
			emailSender.sendMessage(
				_model.getEmail(),
				getEmailAddress(_model.getContact()),
				SUBJECT,
				getMessage(_id));
			logger.info("sent email message to " + _model.getEmail());
			_model.setId(null);
			_model.setInvitationState(InvitationState.SENT);
			update(_id, _model);
			try {
				Thread.sleep(1000);
			} catch (InterruptedException _ex) {
				_ex.printStackTrace();
				throw new InternalServerErrorException(_ex.getMessage());
			}
		}
	}

	/* (non-Javadoc)
	 * @see org.opentdc.invitations.ServiceProvider#register(java.lang.String, java.lang.String)
	 */
	@Override
	public void register(
			String id, 
			String comment) 
				throws NotFoundException,
			ValidationException {
		InvitationModel _invitation = read(id);
		if (_invitation.getInvitationState() == InvitationState.INITIAL) {
			throw new ValidationException("invitation <" + id + "> must be sent before being able to register");
		}
		if (_invitation.getInvitationState() == InvitationState.REGISTERED) {
			logger.warning("invitation <" + id + "> is already registered; ignoring re-registration");
		}
		_invitation.setInvitationState(InvitationState.REGISTERED);
		_invitation.setComment(comment);
		_invitation.setModifiedAt(new Date());
		_invitation.setModifiedBy(getPrincipal());
		update(id, convert(_invitation, true));
		logger.info("register(" + id + ", " + comment + ") -> " + PrettyPrinter.prettyPrintAsJSON(_invitation));
	}

	/* (non-Javadoc)
	 * @see org.opentdc.invitations.ServiceProvider#deregister(java.lang.String, java.lang.String)
	 */
	@Override
	public void deregister(String id, String comment) throws NotFoundException,
			ValidationException {
		InvitationModel _invitation = read(id);
		if (_invitation.getInvitationState() == InvitationState.INITIAL) {
			throw new ValidationException("invitation <" + id + "> must be sent before being able to deregister");
		}
		if (_invitation.getInvitationState() == InvitationState.EXCUSED) {
			logger.warning("invitation <" + id + "> is already excused; ignoring deregistration");
		}
		_invitation.setInvitationState(InvitationState.EXCUSED);
		_invitation.setComment(comment);
		_invitation.setModifiedAt(new Date());
		_invitation.setModifiedBy(getPrincipal());
		update(id, convert(_invitation, true));
		logger.info("deregister(" + id + ", " + comment + ") -> " + PrettyPrinter.prettyPrintAsJSON(_invitation));
	}
	
	private InvitationState convertInvitationState(org.opentdc.events.InvitationState estate) {
		InvitationState _istate = null;
		switch (estate) {
		case INITIAL: _istate =  InvitationState.INITIAL; break;
		case SENT: _istate =  InvitationState.SENT; break;
		case REGISTERED: _istate =  InvitationState.REGISTERED; break; 
		case EXCUSED: _istate =  InvitationState.EXCUSED; break;
		default: _istate =  InvitationState.INITIAL; break;
		}
		return _istate;
	}
	
	private SalutationType convertSalutationType(org.opentdc.events.SalutationType esal) {
		SalutationType _isal = null;
		switch (esal) {
		case HERR: _isal = SalutationType.HERR; break;
		case FRAU: _isal = SalutationType.FRAU; break;
		case DU_M: _isal = SalutationType.DU_M; break;
		case DU_F: _isal = SalutationType.DU_F; break;
		default: _isal = SalutationType.HERR; break;
		}
		return _isal;
	}

	/* (non-Javadoc)
	 * @see org.opentdc.invitations.ServiceProvider#migrate()
	 */
	@Override
	public void migrate() 
			throws InternalServerErrorException {
		org.opentdc.events.mongo.MongodbServiceProvider _sp = 
				new org.opentdc.events.mongo.MongodbServiceProvider(context, "EventsService");
		InvitationModel _invitationModel = null;
		for (EventModel _emodel : _sp.list(null, null, 0, 0)) {
			_invitationModel = new InvitationModel();
			_invitationModel.setId(_emodel.getId());
			_invitationModel.setFirstName(_emodel.getFirstName());
			_invitationModel.setLastName(_emodel.getLastName());
			_invitationModel.setEmail(_emodel.getEmail());
			_invitationModel.setComment(_emodel.getComment());
			// internalComment only exists in InvitationModel, not in EventsModel -> null
			_invitationModel.setContact(_emodel.getContact());
			_invitationModel.setSalutation(convertSalutationType(_emodel.getSalutation()));
			_invitationModel.setInvitationState(convertInvitationState(_emodel.getInvitationState()));			
			_invitationModel.setCreatedAt(_emodel.getCreatedAt());
			_invitationModel.setCreatedBy(_emodel.getCreatedBy());
			_invitationModel.setModifiedAt(new Date());
			_invitationModel.setModifiedBy(getPrincipal());
			create(convert(_invitationModel, true));
			logger.info("create(" + PrettyPrinter.prettyPrintAsJSON(_invitationModel) + ")");

		}
		
	}

	/* (non-Javadoc)
	 * @see org.opentdc.invitations.ServiceProvider#statistics()
	 */
	@Override
	public Properties statistics() {
		int _countEntries = 0;
		int _countInitial = 0;
		int _countSent = 0;
		int _countRegistered = 0;
		int _countExcused = 0;
		int _countComments = 0;
		int _countInternalComments = 0;
		InvitationModel _model = null;
		for (Document doc : list(0, 0)) {
			_model = convert(doc);
			_countEntries++;
			switch(_model.getInvitationState()) {
				case INITIAL:	_countInitial++; break;
				case SENT:		_countSent++; break;
				case REGISTERED: _countRegistered++; break;
				case EXCUSED:	_countExcused++; break;
			}
			if (_model.getComment() != null && ! _model.getComment().isEmpty()) {
				_countComments++;
			}
			if (_model.getInternalComment() != null && ! _model.getInternalComment().isEmpty()) {
				_countInternalComments++;
			}
		}
		Properties _data = new Properties();
		_data.setProperty("entries", new Integer(_countEntries).toString());
		_data.setProperty("initial", new Integer(_countInitial).toString());
		_data.setProperty("sent", new Integer(_countSent).toString());
		_data.setProperty("registered", new Integer(_countRegistered).toString());
		_data.setProperty("excused", new Integer(_countExcused).toString());
		_data.setProperty("comments", new Integer(_countComments).toString());
		_data.setProperty("internalComments", new Integer(_countInternalComments).toString());
		return _data;
	}
}
