# OpenEMR Automated Reminders Implementation Plan

Prepared for rtNOW / Respiratory Therapy Services

Prepared June 2026

Based on the Automated Reminders spreadsheet dated 2026-03-31.

## Recommendation on the Two Draft Files

File 2, rtNOW_OpenEMR_Automation_Plan.docx, is the better foundation. It reads like a realistic implementation plan, stays focused on OpenEMR, asks the right discovery questions, and presents a phased rollout. That is the version most likely to make the hiring manager think you understand both OpenEMR and implementation planning.

File 1, OpenEMR_Automated_Reminders_Implementation_Plan.docx, has useful detail from the spreadsheet, especially around template categories, trigger logic, equipment-specific prep messages, and bilingual routing. However, it overstates the architecture by leading with Python middleware, Celery, Redis, YAML configuration, daemon services, and direct database polling. Those may become useful later, but they should not be the first proposal unless OpenEMR-native capabilities are proven insufficient.

The best final version is therefore File 2 as the planning structure, strengthened with selected concrete details from File 1 and the spreadsheet.

## 1. Executive Summary

rtNOW wants to move a large patient and client communication workflow into OpenEMR so appointment reminders, equipment setup instructions, adherence messages, surveys, and referring-client notifications are triggered automatically instead of being handled manually or through ClickUp.

The correct implementation approach should be OpenEMR-first. I would start by configuring the required OpenEMR fields, appointment/status workflows, patient grouping values, communication templates, and reminder rules. Only after identifying gaps in native OpenEMR behavior would I introduce a lightweight custom module or integration layer for advanced timing, conditional routing, SMS delivery, and audit logging.

The goal is not just to send messages. The goal is to make sure the right patient or client receives the right message, in the right language, at the correct workflow moment, with enough auditability for staff to trust the system.

## 2. What the Spreadsheet Requires

The spreadsheet defines a broad communication library across email and SMS, with English and Spanish versions. The automations are not one simple appointment reminder. They cover the full respiratory therapy journey.
- Appointment scheduling and confirmation for PAP, O2, CPT, NIV, telehealth, and in-office appointment paths.
- 24-hour reminders, day-of reminders, 5-hour reminders, and 4-hour telehealth meeting URL reminders.
- Missed appointment, rescheduled appointment, and canceled appointment notifications.
- PAP/CPAP equipment setup preparation emails with device-specific instructions and video links.
- Equipment-specific branches for devices such as ResMed AirSense/AirCurve, React Health Luna/G3, ResVent iBreeze, Philips DreamStation, and similar PAP equipment variants.
- Sleep Advocate introduction messages for PAP and NIV adherence programs.
- Low usage and compliance-support messages when patients are not meeting expected usage.
- 90-day and 180-day compliance milestone messages.
- Setup and adherence satisfaction survey messages.
- Referring client, DME, and internal notifications for appointment confirmations, cancellations, missed appointments, setup completion, and program completion.

## 3. OpenEMR Configuration Plan

Before building any automation logic, I would make sure OpenEMR has clean structured data for every trigger and template variable. Most automation failures happen because the trigger data is not modeled clearly enough.

### 3.1 Patient and Workflow Fields

I would use OpenEMR Layout Editor, Additional Demographics, Lists, or a custom form/module depending on the current OpenEMR setup. The required fields should include:
- Patient language preference: English or Spanish.
- Patient email, mobile phone, and SMS/email consent.
- Client name or referring organization.
- Client notification email addresses by notification type.
- Appointment setting: telehealth, in-office, or other supported category.
- Meeting URL for telehealth visits.
- rtNOW therapist assigned to the appointment.
- Scheduler employee or scheduling contact.
- Sleep Advocate assigned to the patient.
- PAP equipment model or equipment family.
- Equipment video URL and secondary setup URL where required.
- Actual setup date.
- Setup notes.
- Satisfaction survey URL.

### 3.2 Patient Grouping and Status Values

The spreadsheet depends heavily on patient status changes. I would standardize status values before automation starts so OpenEMR has one source of truth for each workflow step.
- PAP Setup: ready to schedule, scheduled, setup complete, missed appointment, canceled order, rescheduled.
- O2/CPT/NIV scheduling statuses: scheduled, canceled, missed, completed where applicable.
- PAP Adherence: active, low usage, adherence met, adherence not met, program completed.
- NIV Adherence: active, follow-up, 180-day milestone, program completed.
- Appointment outcome values: scheduled, checked in, completed, no-show/missed, canceled, rescheduled.

## 4. Trigger Logic

The automation engine should be organized around trigger types, not around random template names. This makes the system easier to test, maintain, and explain to staff.

### 4.1 Status Change Triggers

These fire when a patient status or grouping changes in OpenEMR. Examples include PAP Setup - Setup, Adherence - Adherence Met, Adherence - Adherence Not Met, PAP Setup - Canceled Order, NIV - Scheduled, CPT - Scheduled, and O2 - Scheduled.

### 4.2 Appointment-Based Triggers

These fire from appointment creation or appointment time. Examples include immediate appointment confirmation, 24-hour reminders, day-of reminders, 5-hour reminders, and 4-hour meeting URL reminders. The logic should branch by appointment type and appointment setting.

### 4.3 Encounter/Event Triggers

These fire when a specific event is recorded in OpenEMR, such as a missed appointment encounter or no-show status. This is important because missed appointment messages should not be sent just because time passed; they should be sent only when the appointment is actually marked missed/no-show.

### 4.4 Date-Based Program Triggers

These fire based on actual setup date or program timeline. Examples include Sleep Advocate introduction after setup, low usage follow-up, 90-day compliance milestone, 180-day NIV milestone, survey messages, and program completion alerts.

### 4.5 Conditional Branching

Each trigger must choose the correct message using structured fields. The key conditions are:
- Language: English or Spanish.
- Communication channel: email, SMS, or both.
- Appointment setting: telehealth vs in-office.
- Service line: PAP, O2, CPT, NIV.
- Equipment model or family for device-specific setup instructions.
- Client/referring organization for routing client notifications.
- Compliance state for adherence-related messages.

## 5. Implementation Approach

### 5.1 Start With OpenEMR-Native Configuration

I would first use OpenEMR-native configuration wherever possible: appointment calendar fields, patient status/grouping lists, custom demographic fields, templates, clinical rules/reminders, and communication logging. This keeps the system understandable to the clinic team and avoids unnecessary custom infrastructure.

### 5.2 Add a Lightweight Custom Layer Only Where Needed

If OpenEMR cannot handle precise timing, complex conditional routing, SMS provider integration, or deduplication, I would add a small custom OpenEMR module or lightweight integration service. I would not begin with Celery, Redis, YAML registries, or a daemon architecture unless volume and timing requirements prove they are necessary.

The custom layer, if needed, should do only four things: evaluate eligible records, select the correct template, send through the approved email/SMS provider, and record an audit trail.

### 5.3 Email and SMS Delivery

Email should be sent through an approved SMTP or transactional email provider such as SendGrid, Mailgun, or the client-approved mail system. SMS should be sent through the selected SMS gateway, such as Twilio, ClickSend, or the vendor already approved by rtNOW. Consent, opt-out behavior, phone validation, and message failure handling must be confirmed before go-live.

### 5.4 Auditability

Every automation should create a traceable record: patient, trigger, template, language, recipient, channel, send time, provider response, success/failure, and retry status. Staff need this because when a patient says they did not receive a reminder, the team must be able to answer from OpenEMR or an admin log.

## 6. Phased Rollout Plan

### Phase 1 - Discovery and Workflow Confirmation
- Confirm OpenEMR version, hosting environment, installed modules, and current communication features.
- Review the spreadsheet with rtNOW staff and confirm which templates are active, duplicated, obsolete, or client-specific.
- Confirm where ClickUp is currently used and whether it will be replaced fully or kept for some tasks.
- Confirm SMS provider, email provider, sender addresses, phone numbers, and compliance requirements.

### Phase 2 - OpenEMR Data Setup
- Create or normalize the required custom fields.
- Create standardized patient grouping/status values.
- Map spreadsheet variables to OpenEMR fields.
- Define missing-data behavior, such as what happens if Meeting URL, language, or equipment model is blank.

### Phase 3 - Appointment Automation Pilot
- Build appointment confirmation, 24-hour reminder, day-of reminder, 4-hour meeting URL, and missed appointment flows.
- Test both telehealth and in-office paths.
- Test English and Spanish templates.
- Validate deduplication so one patient does not receive the same reminder multiple times.

### Phase 4 - Equipment Setup Automations
- Build PAP/CPAP setup prep templates.
- Branch by PAP equipment model and equipment video URLs.
- Test device-specific instructions with sample patient records.

### Phase 5 - Adherence and Program Lifecycle Automations
- Build Sleep Advocate introductions.
- Build low usage and compliance-support messages.
- Build 90-day and 180-day milestone messages.
- Build satisfaction survey and program completion alerts.

### Phase 6 - Client and Internal Notifications
- Build appointment confirmation, cancellation, missed appointment, setup completion, and program completion notifications to the correct client contacts.
- Validate routing per referring organization/DME.
- Confirm CC behavior and internal escalation recipients.

### Phase 7 - UAT and Go-Live
- Run test patients through every major workflow.
- Use a test mode first so messages can be previewed without sending to real patients.
- Get staff sign-off on templates, timing, routing, and audit logs.
- Launch in phases, starting with appointment reminders before adherence and client notifications.

## 7. Testing Checklist
- Template variables render correctly: patient name, client name, therapist, scheduler, meeting URL, setup date, equipment video URL, survey URL.
- Spanish templates are selected only when the patient language requires Spanish.
- Appointment reminders fire at the correct time and timezone.
- Telehealth messages include meeting links; in-office messages use the correct client-facing text.
- Equipment prep messages select the correct equipment-specific instructions.
- Missed/canceled/rescheduled flows only fire after the correct OpenEMR event/status is recorded.
- Client notifications route to the correct DME/referring organization email.
- Duplicate prevention works per patient, appointment, template, and workflow window.
- Failed email/SMS delivery is logged and visible to staff.

## 8. Discovery Questions I Would Ask Before Building
- Which OpenEMR version and hosting model are you using?
- Are appointment records created directly in OpenEMR, or does another scheduler app create them first?
- Where is the Meeting URL generated and stored?
- Which field should be treated as the source of truth for patient workflow status?
- Do patients already have language preference and SMS consent stored in OpenEMR?
- Which email and SMS providers are approved for production use?
- Should ClickUp be fully replaced or used alongside OpenEMR during transition?
- Where does PAP usage/compliance data come from, and is it already available inside OpenEMR?
- Which client/DME contacts should receive each notification type?
- What audit view does staff need when troubleshooting sent or failed reminders?

## 9. Final Recommendation

I would not lead with a heavy custom architecture. I would lead with an OpenEMR-first implementation plan, then add a focused custom module or integration only for the gaps OpenEMR cannot reliably handle: precise timing, complex conditional routing, SMS delivery, deduplication, and audit logging.

This approach shows practical OpenEMR knowledge, respects the client workflow, reduces implementation risk, and still leaves room for custom engineering where it creates real value.